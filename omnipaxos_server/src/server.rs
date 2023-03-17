use hocon::HoconLoader;
use omnipaxos_core::{
    omni_paxos::{OmniPaxos, OmniPaxosConfig},
    storage::{Entry, Snapshot, StopSign, Storage},
    util::{ConfigurationId, NodeId},
};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    cmp,
    fs,
};

use anyhow::Error;
use commitlog::LogOptions;
use futures::StreamExt;
use log::*;
use sled::Config;
use tokio::time;

use crate::{
    message::{
        NodeMessage::{*, self},
        log_migration::{*, self},
        client::{*, self}
    },
    router::Router,
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD, WAIT_LEADER_TIMEOUT, MIGRATTION_BATCH_SIZE},
};

pub struct LogMigrationState {
    // for new servers
    pub response_to_receive: u32,
    pub leader: NodeId,
    pub decided_idx: u64, // # of logs to pull 

    // for leader who creates new servers
    pub new_servers: Vec<NodeId>,
    pub servers_to_pull_log: u32,
}

impl Default for LogMigrationState {
    fn default() -> Self {
        Self {
            response_to_receive: 0,
            leader: 0,
            decided_idx: 0,
            servers_to_pull_log: 0,
            new_servers: Vec::new(),
        }
    }
}

pub struct OmniPaxosServer<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    id: NodeId,
    router: Router,
    omnipaxos_instances: HashMap<ConfigurationId, Arc<Mutex<OmniPaxos<T, S, B>>>>,
    configs: HashMap<ConfigurationId, OmniPaxosConfig>,
    first_connects: HashMap<NodeId, bool>,
    active_instance: ConfigurationId,
    log_migration_state: LogMigrationState,
}

impl<T, S, B> OmniPaxosServer<T, S, B>
where
    T: Entry,
    S: Snapshot<T>,
    B: Storage<T, S>,
{
    pub fn new(
        id: NodeId,
        router: Router,
        configs: Vec<OmniPaxosConfig>,
    ) -> Self {
        let mut omnipaxos_instances = HashMap::new();
        let mut config_map = HashMap::new();
        let mut active_instance = 0;

        for config in configs {
            let config_id = config.configuration_id;

            // Persistent Log config
            let log_path = format!(
                "{}node{}/config_{}",
                config.logger_file_path.as_ref().unwrap(),
                config.pid,
                config_id
            );
            let my_logopts = LogOptions::new(log_path.clone());
            let my_sled_opts = Config::new().path(log_path.clone());
            let my_config = PersistentStorageConfig::with(log_path, my_logopts, my_sled_opts);

            // Create OmniPaxos instance
            let omnipaxos_instance: Arc<Mutex<OmniPaxos<T, S, B>>> = Self.create_instance(&config);
            if let None = omnipaxos_instance.lock().unwrap().is_reconfigured() {
                active_instance = config_id;
            }
            // Request prepares if failed
            let exists_persistent_log = Path::new(&log_path).is_dir();
            if exists_persistent_log {
                omnipaxos_instance.lock().unwrap().fail_recovery();
            }
            omnipaxos_instances.insert(config_id, omnipaxos_instance);
            config_map.insert(config_id, config);
        }

        OmniPaxosServer {
            id,
            router,
            omnipaxos_instances,
            configs: HashMap::new(), // config_map?
            first_connects: HashMap::new(),
            active_instance,
            log_migration_state: LogMigrationState::default(),
        }
    }

    fn handle_incoming_msg(&mut self, in_msg: Result<NodeMessage<T, S>, Error>) {
        trace!("Receiving: {:?}", in_msg);
        match in_msg {
            // New node connection
            Ok(Hello(id)) => {
                if self.first_connects.get(&id).is_some() {
                    let relevant_configs = self
                        .configs
                        .iter()
                        .filter(|(_, config)| config.peers.contains(&id));
                    for (cid, _) in relevant_configs {
                        self.omnipaxos_instances
                            .get(cid)
                            .unwrap()
                            .lock()
                            .unwrap()
                            .reconnected(id);
                    }
                } else {
                    self.first_connects.insert(id, true);
                }
            },
            // Pass message to instance
            Ok(OmniPaxosMessage(cid, msg)) => {
                // TODO: Remove when we dont need to debug anymore
                if let omnipaxos_core::messages::Message::SequencePaxos(m) = msg.clone() {
                    debug!("Receiving PaxosMessage: {:?}", m);
                }
                if let omnipaxos_core::messages::Message::BLE(m) = msg.clone() {
                    debug!("Receiving BLEMessage: {:?}", m);
                }

                if let Some(instance) = self.omnipaxos_instances.get(&cid) {
                    instance.lock().unwrap().handle_incoming(msg);
                }
            },
            Ok(LogMigrationMessage(logMigrationMessage)) => {
                self.handle_incoming_log_migration_msg(logMigrationMessage);
            },
            Ok(ClientMessage(clientMessage)) => {
                self.handle_incoming_client_msg(clientMessage);
            },
            Ok(_) => unimplemented!(),
            Err(err) => warn!("Could not deserialize message: {}", err),
            
        }
    }

    fn handle_incoming_client_msg(&mut self, clientMessage: client::ClientMessage<T>) {
        let client::ClientMessage{configuration_id, msg} = clientMessage;
        match msg {
            // Append to log of corrrect instance
            Append { entry } => {
                debug!("Got an append request from a client");
                let append_attempt = self.omnipaxos_instances
                    .get(&configuration_id)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .append(entry);
                if let Err(_) = append_attempt {
                    warn!("Append failed");
                }
            }
            // Reconfiguration request 
            Reconfigure { request } => {
                debug!("Got a reconfig request from a client");
                self.omnipaxos_instances
                    .get(&self.active_instance)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .reconfigure(request);
            }
        }
    }

    async fn handle_incoming_log_migration_msg(&mut self, logMigrationMessage: log_migration::LogMigrationMessage<T, S>) {
        let log_migration::LogMigrationMessage{configuration_id, from, to, msg} = logMigrationMessage;
        match msg {
            LogPullStart { des_servers, decided_idx } => {
                self.log_migration_state.response_to_receive = (decided_idx + MIGRATTION_BATCH_SIZE - 1) / MIGRATTION_BATCH_SIZE;
                self.log_migration_state.leader = from;
                self.log_migration_state.decided_idx = decided_idx;

                let cur_idx = 0;
                for des_server in des_servers {
                    if cur_idx >= decided_idx {
                        break;
                    }
                    let batch_size = cmp::min(MIGRATTION_BATCH_SIZE, decided_idx-cur_idx);
                    let out_msg =
                        log_migration::LogMigrationMessage::<T, S> {
                            configuration_id: configuration_id,
                            from: self.id,
                            to: des_server,
                            msg: LogPullRequest{from_idx: cur_idx, to_idx: cur_idx+batch_size}
                        };
                    match self
                        .router
                        .send_message(des_server, LogMigrationMessage(out_msg))
                        .await {
                            Err(err) => trace!("Error sending LogPullRequest from {} to {}", self.id, out_msg.get_receiver()),
                            _ => {}, // success
                        }
                    cur_idx += batch_size;
                }
            }
            LogPullRequest { from_idx, to_idx } => {
                // get KVs
                let logs = self.omnipaxos_instances
                    .get(&configuration_id)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .read_decided_suffix(from_idx);

                // extract [from_idx..to_idx)
                if let Some(logs_vec) = logs {
                    let logs_vec_slice = logs_vec[..(to_idx-from_idx)];
                    let out_msg =
                        log_migration::LogMigrationMessage::<T, S> {
                            configuration_id: configuration_id,
                            from: self.id,
                            to: from,
                            msg: LogPullResponse::<T, S> {
                                from_idx: from_idx,
                                to_idx: to_idx,
                                logs: logs_vec_slice
                            }
                        };
                    match self
                        .router
                        .send_message(from, LogMigrationMessage(out_msg))
                        .await {
                            Err(err) => trace!("Error sending LogPullResponse from {} to {}", self.id, out_msg.get_receiver()),
                            _ => {},
                        }
                }
            }
            LogPullResponse::<T, S> { from_idx, to_idx, logs } => {
                for log_entry in logs {
                    self.omnipaxos_instances
                        .get(&configuration_id)
                        .unwrap()
                        .lock()
                        .unwrap()
                        .append(log_entry)
                        .expect("Failed on append");
                }
                self.log_migration_state.response_to_receive -= 1;

                // 
                if self.log_migration_state.response_to_receive == 0 {
                    let out_msg =
                        log_migration::LogMigrationMessage::<T, S> {
                            configuration_id: configuration_id,
                            from: self.id,
                            to: self.log_migration_state.leader,
                            msg: LogPullOneDone {
                                get_idx: self.log_migration_state.decided_idx,
                            }
                        };
                    match self
                        .router
                        .send_message(from, LogMigrationMessage(out_msg))
                        .await {
                            Err(err) => trace!("Error sending LogPullOneDone from {} to {}", self.id, out_msg.get_receiver()),
                            _ => {},
                        }
                    
                    self.log_migration_state = LogMigrationState::default();
                }
            }
            LogPullOneDone { get_idx } => {
                self.log_migration_state.num_new_servers -= 1;

                //
                if self.log_migration_state.num_new_servers == 0 {
                    // let mut peers = self.configs
                    //     .get(&configuration_id)
                    //     .unwrap()
                    //     .peers;
                    // peers.push(self.id);

                    for new_servers in self.log_migration_state.new_servers {
                        let out_msg =
                            log_migration::LogMigrationMessage::<T, S> {
                                configuration_id: configuration_id+1, // new configuration!!
                                from: self.id,
                                to: self.log_migration_state.leader,
                                msg: StartNewConfiguration {
                                    new_servers: self.log_migration_state.new_servers
                                }
                            };
                        match self
                            .router
                            .send_message(from, LogMigrationMessage(out_msg))
                            .await {
                                Err(err) => trace!("Error sending StartNewConfiguration from {} to {}", self.id, out_msg.get_receiver()),
                                _ => {},
                            }
                    }

                    self.log_migration_state = LogMigrationState::default();
                } 
            }
            StartNewConfiguration { new_servers } => {
                // Create and save new config file
                let config_id = configuration_id;
                let peers = new_servers
                    .into_iter()
                    .filter(|&id| id != self.id)
                    .collect();

                let config_str = format!(r#"{{
                    config_id: {},
                    pid: {},
                    peers: {:?},
                    log_file_path: "logs/"
                }}
                "#, config_id, self.id, peers);

                let config_file_path = format!(
                    "config/node{}/c{}.conf",
                    self.id,
                    config_id
                );
                fs::write(config_file_path.clone(), config_str).expect("Couldn't write new config file");

                // TODO: Need to update routers addresses with new addresses
                // parsed from the metadata of the stopsign with self.router.add_address(). 
                // For now all addresses are hard coded in main.rs
                
                // Create new OmniPaxos instance
                let cfg = HoconLoader::new()
                    .load_file(config_file_path)
                    .expect("Failed to load hocon file")
                    .hocon()
                    .unwrap();
                let config = OmniPaxosConfig::with_hocon(&cfg);
                let new_instance = Self.create_instance(&config);
                self.omnipaxos_instances.insert(config_id, new_instance);

                // New instance is now active
                self.active_instance = config_id;

            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        for (config_id, instance) in self.omnipaxos_instances.iter() {
            let messages = instance.lock().unwrap().outgoing_messages();
            for msg in messages {
                let receiver = msg.get_receiver();
                trace!("Sending to {}: {:?}", receiver, msg);
                match self
                    .router
                    .send_message(receiver, OmniPaxosMessage(*config_id, msg))
                    .await {
                    Err(err) => trace!("Error sending message to node {}, {}", receiver, err),
                    _ => {
                        // TODO: Remove when we dont need to debug anymore
                        //if let omnipaxos_core::messages::Message::SequencePaxos(m) = msg.clone() {
                        //    debug!("Sending to {} : {:?}", receiver, m);
                        //}
                    },
                }
            }
        }
    }

    fn handle_election_timeout(&self) {
        for (_, instance) in self.omnipaxos_instances.iter() {
            instance.lock().unwrap().election_timeout();
        }
    }

    fn debug_state(&self) {
        for (config_id, instance) in self.omnipaxos_instances.iter() {
            let mut entries = vec![];
            let mut i = 0;
            while let Some(entry) = instance.lock().unwrap().read(i) {
                entries.push(entry);
                i += 1;
            }
            debug!("C{} LOG ENTRIES: {:?}", config_id, entries);
            debug!(
                "C{} Leader = {:?}",
                config_id,
                instance.lock().unwrap().get_current_leader()
            );
        }
    }

    fn handle_reconfig(&mut self) {
        if self.active_instance == 0 {
            return;
        }

        let stopsign = self.omnipaxos_instances
            .get(&self.active_instance)
            .unwrap()
            .lock()
            .unwrap()
            .is_reconfigured();

        // Active instance is inactive, crate new config
        if let Some(stopsign) = stopsign {
            debug!("Stopsign detected! Creating new instance");

            // TODO!! if itself is leader, send LogPullStart to all the new servers
            let leader_id = self.omnipaxos_instances
                .get(&self.active_instance)
                .unwrap()
                .lock()
                .unwrap()
                .get_current_leader()
                .unwrap();

            let decided_idx = self.omnipaxos_instances
                .get(&self.active_instance)
                .unwrap()
                .lock()
                .unwrap()
                .get_decided_idx();

            if leader_id != self.id {
                return;
            }

            // TODO!! if itself is leader, send LogPullStart to all the new servers
            let old_servers = self.configs
                .get(&self.active_instance)
                .unwrap()
                .peers;
            old_servers.append(self.id);

            self.log_migration_state.new_servers = stopsign.nodes; // all the servers in the next configuration
        
            let new_servers_to_pull_log: Vec<u64> = self.log_migration_state.new_servers
                .iter()
                .filter(|&id| !old_servers.contains(id))
                .cloned()
                .collect();

            for new_server_to_pull_log in new_servers_to_pull_log {
                let out_msg =
                    log_migration::LogMigrationMessage::<T, S> {
                        configuration_id: stopsign.config_id - 1, // new configuration!!
                        from: self.id,
                        to: new_server_to_pull_log,
                        msg: LogPullStart {
                            des_servers: old_servers,
                            decided_idx: decided_idx
                        }
                    };
                match self
                    .router
                    .send_message(from, LogMigrationMessage(out_msg))
                    .await {
                        Err(err) => trace!("Error sending LogPullStart from {} to {}", self.id, out_msg.get_receiver()),
                        _ => {},
                    }
            }
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        let mut config_check_interval = time::interval(WAIT_LEADER_TIMEOUT);

        loop {
            tokio::select! {
                biased;

                _ = election_interval.tick() => { self.handle_election_timeout(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.router.next() => { self.handle_incoming_msg(in_msg); },
                _ = config_check_interval.tick() => { 
                    self.debug_state(); // TODO: remove later
                    self.handle_reconfig();
                },
                else => { }
            }
        }
    }

    fn create_instance(config: &OmniPaxosConfig) -> Arc<Mutex<OmniPaxos<T, S, B>>> {
        // Persistent Log config
        let log_path = format!(
            "{}node{}/config_{}",
            config.logger_file_path.as_ref().unwrap(),
            config.pid,
            config.configuration_id
            );
        let exists_persistent_log = Path::new(&log_path).is_dir();
        let my_logopts = LogOptions::new(log_path.clone());
        let my_sled_opts = Config::new().path(log_path.clone());
        let my_config = PersistentStorageConfig::with(log_path, my_logopts, my_sled_opts);

        // Create OmniPaxos instance
        let instance = Arc::new(Mutex::new(config.clone().build(PersistentStorage::open(my_config))));
        // Request prepares if failed
        if exists_persistent_log {
            instance.lock().unwrap().fail_recovery();
        }
        return instance;
    }
}
