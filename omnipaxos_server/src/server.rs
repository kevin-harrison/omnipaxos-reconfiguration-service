use hocon::HoconLoader;
use omnipaxos_core::{
    omni_paxos::{OmniPaxos, OmniPaxosConfig},
    storage::Snapshot,
    util::{ConfigurationId, NodeId, LogEntry},
};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Error;
use commitlog::LogOptions;
use futures::StreamExt;
use log::*;
use sled::Config;
use tokio::time;

use crate::{
    kv::{KVSnapshot, KeyValue},
    message::{
        log_migration::{
            LogMigrationMsg::*,
            *,
        },
        ClientMsg::{self, *},
        NodeMessage::{self, *},
    },
    router::Router,
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD, WAIT_LEADER_TIMEOUT},
};

struct LogMigrationState {
    // for new servers
    responses_left: usize,
    snapshots: Vec<Option<KVSnapshot>>,    
    config_file: String,

    // for leader who creates new servers
    //new_servers: Vec<NodeId>,
    //servers_to_pull_log: u32,
}

impl LogMigrationState {
    fn get_full_snapshot(self) -> KVSnapshot {
        // Merge snapshots
        let mut it = self.snapshots.into_iter();
        let mut full_snapshot = it.next().unwrap().unwrap();
        for snapshot in it {
            full_snapshot.merge(snapshot.unwrap());
        }
        return full_snapshot;
    }
}


type OmniPaxosInstance = OmniPaxos<KeyValue, KVSnapshot, PersistentStorage<KeyValue, KVSnapshot>>;

/// Configuration for `OmniPaxos`.
/// # Fields
/// * `id`: The identifier for the server cluster that this server is a part of.
/// * `router`: Listens for requests from clients and maintains TCP connections with other servers in the cluster.
/// * `omnipaxos_instances`: Holds the omnipaxos instances cooresponding to each server configuration it has been part of.
/// * `configs`: The configs for each instance in `omnipaxos_instances`.
/// * `first_connects`: Keeps track of when another node in the cluster creates its first connection, vs a reconnection.
/// * `active_instance`: The configuration this server is a part of which does not have a stopsign.
/// * `log_migration_state`: Used to store the missing state necessary if joining a cluster in a config later than the first.
pub struct OmniPaxosServer {
    id: NodeId,
    router: Router,
    omnipaxos_instances: HashMap<ConfigurationId, Arc<Mutex<OmniPaxosInstance>>>,
    configs: HashMap<ConfigurationId, OmniPaxosConfig>,
    first_connects: HashMap<NodeId, bool>,
    active_instance: ConfigurationId,
    log_migration_state: Option<LogMigrationState>,
}

impl OmniPaxosServer {
    pub fn new(id: NodeId, router: Router, configs: Vec<OmniPaxosConfig>) -> Self {
        let mut omnipaxos_instances = HashMap::new();
        let mut config_map = HashMap::new();
        let mut active_instance = 0;

        for config in configs {
            let config_id = config.configuration_id;

            // Create OmniPaxos instance from config
            let omnipaxos_instance = OmniPaxosServer::create_instance(&config);
            // Check if instance is the active one
            if let None = omnipaxos_instance.lock().unwrap().is_reconfigured() {
                active_instance = config_id;
            }

            omnipaxos_instances.insert(config_id, omnipaxos_instance);
            config_map.insert(config_id, config);
        }

        OmniPaxosServer {
            id,
            router,
            omnipaxos_instances,
            configs: config_map,
            first_connects: HashMap::new(),
            active_instance,
            log_migration_state: None,
        }
    }

    async fn handle_incoming_msg(&mut self, in_msg: Result<NodeMessage, Error>) {
        trace!("Receiving: {:?}", in_msg);
        match in_msg {
            // New node connection
            Ok(Hello(id)) => {
                if self.first_connects.get(&id).is_some() {
                    // Call reconnected() on instances if node is reconnecting
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
            }
            // Pass message to instance
            Ok(OmniPaxosMessage(cid, msg)) => {
                if let Some(instance) = self.omnipaxos_instances.get(&cid) {
                    instance.lock().unwrap().handle_incoming(msg);
                }
            }
            Ok(ClientMessage(client_msg)) => {
                self.handle_incoming_client_msg(client_msg);
            }
            Ok(LogMigrateMessage(log_msg)) => {
                self.handle_incoming_log_migration_msg(log_msg).await;
            }
            Err(err) => warn!("Could not deserialize message: {}", err),
        }
    }

    fn handle_incoming_client_msg(&mut self, msg: ClientMsg) {
        match msg {
            // Append to log of corrrect instance
            Append(config_id, kv) => {
                debug!("Got an append request from a client");
                if let Some(instance) = self.omnipaxos_instances.get(&config_id) {
                    if let Err(_) = instance.lock().unwrap().append(kv) {
                        warn!("Append request failed");
                    }
                }                       
             }
            // Reconfiguration request
            Reconfigure(request) => {
                debug!("Got a reconfig request from a client {:?}", request);
                if let Some(instance) = self.omnipaxos_instances.get(&1) {
                    if let Err(_) = instance.lock().unwrap().reconfigure(request) {
                        warn!("Reconfig request failed");
                    }
                }
            }
        }
    }

    async fn handle_incoming_log_migration_msg(&mut self, migration_msg: LogMigrationMessage) {
        let LogMigrationMessage {
            configuration_id,
            from,
            to,
            msg,
        } = migration_msg;

        match msg {
            LogPullStart(PullStart { config_nodes, pull_from }) => {
                self.request_logs(configuration_id, config_nodes, pull_from).await;
            }
            LogPullRequest(PullRequest { chunk_index, num_chunks }) => {
                // Get snapshots of prev configs
                let mut snapshots: Vec<KVSnapshot> = vec![];
                for config in 1..configuration_id {
                    let instance = self.omnipaxos_instances.get(&config).unwrap();
                    // Snapshotting is bugged if we include StopSign so do decided_idx - 1
                    let decided_index = instance.lock().unwrap().get_decided_idx();
                    instance.lock().unwrap().snapshot(Some(decided_index - 1), true).unwrap();
                    if let LogEntry::Snapshotted(s) = instance.lock().unwrap().read_decided_suffix(0).unwrap()[0].clone() {
                        snapshots.push(s.snapshot);
                    }
                }

                // Merge snapshots
                let mut it = snapshots.into_iter();
                let mut full_snapshot = it.next().unwrap();
                for snapshot in it {
                    full_snapshot.merge(snapshot);
                }
                
                // Take chunk of snapshot
                let chunk = full_snapshot.get_chunk(chunk_index, num_chunks);
                
                // Send chunk to requestee
                let out_msg = LogMigrationMessage {
                    configuration_id,
                    from: self.id,
                    to: from,
                    msg: LogPullResponse(PullResponse {
                        chunk_index,
                        snapshot_chunk: chunk,
                    }),
                };    
                debug!("Sending PullResponse to {}: {:?}", from, out_msg);
                let msg_sent = self.router.send_message(out_msg.get_receiver(), LogMigrateMessage(out_msg)).await;
                if let Err(_) = msg_sent {
                    error!("Error sending LogPullResponse from {} to {}", self.id, from);
                }


            }
            LogPullResponse(PullResponse { chunk_index, snapshot_chunk }) => {
                debug!("Received snapshot chunk {}: {:?}", chunk_index, snapshot_chunk);
            
                let state = self.log_migration_state.as_mut().unwrap();
                state.snapshots[chunk_index] = Some(snapshot_chunk);
                state.responses_left -= 1;

                // Migration completed
                if state.responses_left == 0 {
                    debug!("Log Migration complete");
                    // Create new OmniPaxos instance
                    let cfg = HoconLoader::new()
                        .load_file(state.config_file.clone())
                        .expect("Failed to load hocon file")
                        .hocon()
                        .unwrap();
                    let config = OmniPaxosConfig::with_hocon(&cfg);
                    let new_instance = OmniPaxosServer::create_instance(&config);
                    self.omnipaxos_instances.insert(configuration_id, new_instance);

                    // New instance is now active
                    self.active_instance = configuration_id;
                }
            }
            _ => () // TODO: other messages
        }

    }

    async fn request_logs(&mut self, new_config: ConfigurationId, config_nodes: Vec<NodeId>, pull_from: Vec<NodeId>) {
        // Create and save new config file
        let peers: Vec<NodeId> = config_nodes
            .into_iter()
            .filter(|&id| id != self.id)
            .collect();
        let config_file_path = 
            OmniPaxosServer::create_config(new_config, self.id, &peers);
        
        // Initialize log migration state
        let num_responses = pull_from.len();
        self.log_migration_state = Some(LogMigrationState { 
            responses_left: num_responses,
            snapshots: vec![None; num_responses], 
            config_file: config_file_path,
        });
        
        // Request snapshot parts from pull_from nodes
        for (chunk_index, &pull_node) in pull_from.iter().enumerate() {
            let out_msg = LogMigrationMessage {
                configuration_id: new_config,
                from: self.id,
                to: pull_node,
                msg: LogPullRequest(PullRequest {
                    chunk_index,
                    num_chunks: pull_from.len()
                }),
            };
            debug!("Sending PullRequest to {}: {:?}", pull_node, out_msg);
            if let Err(_) = self.router.send_message(pull_node, LogMigrateMessage(out_msg)).await {
                error!("Error sending LogPullRequest from {} to {}", self.id, pull_node);
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
                    .await
                {
                    Err(err) => trace!("Error sending message to node {}, {}", receiver, err),
                    _ => {
                    }
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
            //debug!("C{} LOG ENTRIES: {:?}", config_id, entries);
            debug!("C{} Decided entries: {:?}", config_id, instance.lock().unwrap().read_decided_suffix(0));
            debug!(
                "C{} Leader = {:?}",
                config_id,
                instance.lock().unwrap().get_current_leader()
            );
        }
        //debug!("Active config = {}", self.active_instance);
        println!();
    }

    async fn check_for_reconfig(&mut self) {
        if self.active_instance == 0 {
            return;
        }

        let stopsign = self
            .omnipaxos_instances
            .get(&self.active_instance)
            .unwrap()
            .lock()
            .unwrap()
            .is_reconfigured();

        // Active instance is inactive, crate new config
        if let Some(stopsign) = stopsign {
            debug!("Stopsign detected! Creating new instance");

            // Create and save new config file
            let config_id = stopsign.config_id;
            let next_nodes: Vec<NodeId> = stopsign.nodes.clone();
            let peers: Vec<NodeId> = stopsign.nodes
                .clone()
                .into_iter()
                .filter(|&id| id != self.id)
                .collect();
            let config_file_path = 
                OmniPaxosServer::create_config(config_id, self.id, &peers);

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
            let new_instance = OmniPaxosServer::create_instance(&config);
            self.omnipaxos_instances.insert(config_id, new_instance);

            // New instance is now active
            let prev_instance = self.active_instance;
            self.active_instance = config_id;

            // Only the leader tells new servers to request
            // TODO: Some network conditions can break this
            // (no leader or partition or non-perfect links)
            let leader_id = self
                .omnipaxos_instances
                .get(&prev_instance)
                .unwrap()
                .lock()
                .unwrap()
                .get_current_leader()
                .unwrap();
            if self.id != leader_id {
                return;
            }

            // Identify new servers
            let mut old_servers = self.configs.get(&prev_instance).unwrap().peers.clone();
            old_servers.push(self.id);
            let new_servers: Vec<NodeId> = stopsign.nodes
                .clone()
                .into_iter()
                .filter(|&id| !old_servers.contains(&id))
                .collect();

            // Tell new servers to request logs
            for new_server in new_servers {
                let pull_from: Vec<NodeId> = if let Some(metadata_vec) = stopsign.metadata.clone() {
                    bincode::deserialize(&metadata_vec[..]).unwrap()
                } else {
                    next_nodes.clone().into_iter().filter(|&id| id != new_server).collect()
                };

                let out_msg = LogMigrationMessage {
                    configuration_id: stopsign.config_id,
                    from: self.id,
                    to: new_server,
                    msg: LogPullStart(PullStart {
                        config_nodes: next_nodes.clone(),
                        pull_from: pull_from,
                    }),
                };
                debug!("Sending Pull start to {}: {:?}", new_server, out_msg);
                let msg_sent = self.router.send_message(out_msg.get_receiver(), LogMigrateMessage(out_msg)).await;
                if let Err(err) = msg_sent {
                    error!("Error sending LogPullStart from {} to {}: {:?}", self.id, new_server, err);
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
                Some(in_msg) = self.router.next() => { self.handle_incoming_msg(in_msg).await; },
                _ = config_check_interval.tick() => {
                    self.debug_state(); // TODO: remove later
                    self.check_for_reconfig().await;
                },
                else => { }
            }
        }
    }

    fn create_instance(config: &OmniPaxosConfig) -> Arc<Mutex<OmniPaxosInstance>> {
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
        let instance = Arc::new(Mutex::new(
            config.clone().build(PersistentStorage::open(my_config)),
        ));
        // Request prepares if failed
        if exists_persistent_log {
            instance.lock().unwrap().fail_recovery();
        }
        return instance;
    }

    fn create_config(config_id: ConfigurationId, own_id: NodeId, peers: &Vec<NodeId>) -> String {
        let config_str = format!(
            r#"{{
                config_id: {},
                pid: {},
                peers: {:?},
                log_file_path: "logs/"
            }}
            "#,
            config_id, own_id, peers
            );

        let config_file_path = format!("config/node{}/c{}.conf", own_id, config_id);
        fs::write(config_file_path.clone(), config_str)
            .expect("Couldn't write new config file");
        return config_file_path; 
    }
}
