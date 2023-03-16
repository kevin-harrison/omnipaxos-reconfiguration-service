use hocon::HoconLoader;
use omnipaxos_core::{
    omni_paxos::{OmniPaxos, OmniPaxosConfig},
    util::{ConfigurationId, NodeId},
};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    fs,
};

use anyhow::Error;
use commitlog::LogOptions;
use futures::StreamExt;
use log::*;
use sled::Config;
use tokio::time;

use crate::{
    kv::{KVSnapshot, KeyValue},
    message::{NodeMessage::{self, *}, log_migration::{LogMigrationMsg::{self, *}}},
    router::Router,
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD, WAIT_LEADER_TIMEOUT},
};

pub struct OmniPaxosServer {
    id: NodeId,
    router: Router,
    omnipaxos_instances: HashMap<ConfigurationId, OmniPaxosInstance>,
    configs: HashMap<ConfigurationId, OmniPaxosConfig>,
    first_connects: HashMap<NodeId, bool>,
    active_instance: ConfigurationId
}

type OmniPaxosInstance = Arc<Mutex<OmniPaxos<KeyValue, KVSnapshot, PersistentStorage<KeyValue, KVSnapshot>>>>;

impl OmniPaxosServer {
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
            // Create OmniPaxos instance
            let omnipaxos_instance: OmniPaxosInstance = create_instance(&config);
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
            configs: HashMap::new(),
            first_connects: HashMap::new(),
            active_instance
        }
    }



    fn handle_incoming_msg(&mut self, in_msg: Result<NodeMessage, Error>) {
        trace!("Receiving: {:?}", in_msg);
        match in_msg {
            // Pass message to instance
            Ok(OmniPaxosMessage(cid, msg)) => {
                // TODO: Remove when we dont need to debug anymore
                if let omnipaxos_core::messages::Message::SequencePaxos(m) = msg.clone() {
                    debug!("Receiving: {:?}", m);
                }
                if let Some(instance) = self.omnipaxos_instances.get(&cid) {
                    instance.lock().unwrap().handle_incoming(msg);
                }
            },
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
            // Append to log request 
            Ok(Append(cid, kv)) => {
                debug!("Got an append request from a client");
                let append_attempt = self.omnipaxos_instances
                    .get(&cid)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .append(kv); 
                if let Err(_) = append_attempt {
                    warn!("Append failed");
                }
            },
            // Reconfiguration request 
            Ok(LogMigrationMessage(StartNewConfiguration(rc))) => {
                debug!("Got a reconfig request from a client");
                self.omnipaxos_instances
                    .get(&self.active_instance)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .reconfigure(rc);
            },
            Ok(_) => unimplemented!(),
            Err(err) => warn!("Could not deserialize message: {}", err),
            
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
            
            // Create and save new config file
            let config_id = stopsign.config_id;
            let peers: Vec<NodeId> = stopsign
                .nodes
                .into_iter()
                .filter(|&x| x != self.id).collect();

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
            let new_instance = create_instance(&config);
            self.omnipaxos_instances.insert(config_id, new_instance);

            // New instance is now active
            self.active_instance = config_id;
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

        fn handle_pull_start() {

        }

        fn handle_pull_request() {

        }

        fn handle_pull_reponse() {

        }

        fn handle_pull_done() {

        }

        fn start_new_configuration() {

        }



    }
}

fn create_instance(config: &OmniPaxosConfig) -> OmniPaxosInstance {
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
