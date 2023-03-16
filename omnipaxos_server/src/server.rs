use omnipaxos_core::{
    util::{NodeId, LogEntry, ConfigurationId}, 
    omni_paxos::{OmniPaxosConfig, OmniPaxos}};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex}, 
    net::SocketAddr,
    path::Path,
};

use tokio::time;
use futures::StreamExt;
use log::*;
use anyhow::Error;
use commitlog::LogOptions;
use sled::{Config};

use crate::{
    router::Router, 
    message::NodeMessage::{*, self}, 
    kv::{KeyValue, KVSnapshot},
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD, WAIT_LEADER_TIMEOUT},
};

pub struct OmniPaxosServer {
    router: Router,
    omnipaxos_instances: HashMap<ConfigurationId, OmniPaxosInstance>,
    configs: HashMap<ConfigurationId, OmniPaxosConfig>,
    first_connects: HashMap<NodeId, bool>,
}

type OmniPaxosInstance = Arc<Mutex<OmniPaxos<KeyValue, (), PersistentStorage<KeyValue, ()>>>>;

impl OmniPaxosServer {
    pub async fn new(addresses: HashMap<NodeId, SocketAddr>, configs: Vec<OmniPaxosConfig>) -> Self {
        let mut omnipaxos_instances = HashMap::new();
        let mut config_map = HashMap::new(); 
        let mut id = None;

        for config in configs {          
            let config_id = config.configuration_id;
            id = Some(config.pid);

            // Persistent Log config
            let log_path = format!("{}/node{}/config_{}", config.logger_file_path.as_ref().unwrap(), config.pid, config_id);
            let exists_persistent_log = Path::new(&log_path).is_dir();
            let my_logopts = LogOptions::new(log_path.clone());
            let my_sled_opts = Config::new().path(log_path.clone());
            let my_config = PersistentStorageConfig::with(log_path, my_logopts, my_sled_opts);

            // Create OmniPaxos instance
            let omnipaxos_instance: OmniPaxosInstance = 
                Arc::new(Mutex::new(config.clone().build(PersistentStorage::open(my_config))));
            // Request prepares if failed
            if exists_persistent_log {
                omnipaxos_instance.lock().unwrap().fail_recovery();
            }
            omnipaxos_instances.insert(config_id, omnipaxos_instance);
            config_map.insert(config_id, config);
        }
        
        // Create router to handle node TCP connections
        let id = id.unwrap();
        let listen_address = addresses.get(&id).unwrap().clone(); 
        let router: Router = Router::new(id, listen_address, addresses.clone()).await.unwrap();

        OmniPaxosServer { 
            router, 
            omnipaxos_instances, 
            configs: HashMap::new(), 
            first_connects: HashMap::new()
        }
    }

    fn handle_incoming_msg(&mut self, in_msg: Result<NodeMessage, Error>) {
        debug!("Receiving: {:?}", in_msg);
        match in_msg {
            // Pass message to correct instance
            Ok(OmniPaxosMessage(cid, msg)) => {
                self.omnipaxos_instances
                    .get(&cid)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .handle_incoming(msg)
            },
            // On node reconnect call reconnect() on relevant instances 
            Ok(Hello(id)) => {
                if self.first_connects.get(&id).is_some() {
                    let relevant_configs = self.configs
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
            // Append to log of corrrect instance
            Ok(Append(cid, kv)) => {
                debug!("Got an append request from a client");
                self.omnipaxos_instances
                    .get(&cid)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .append(kv)
                    .expect("Failed on append");
            },
            Err(err) => warn!("Could not deserialize message: {}", err)
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        for (config_id, instance) in self.omnipaxos_instances.iter() {
            let messages = instance.lock().unwrap().outgoing_messages();
            for msg in messages {
                let receiver = msg.get_receiver();
                trace!("Sending to {}: {:?}", receiver, msg);
                match self.router.send_message(receiver, OmniPaxosMessage(*config_id, msg)).await {
                    Err(err) => warn!("Error sending message to node {}, {}", receiver, err),
                    _ => ()
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
            let mut entries: Vec<LogEntry<KeyValue, ()>> = vec![];
            let mut i = 0;
            while let Some(entry) = instance.lock().unwrap().read(i) {
                entries.push(entry);
                i += 1;
            }
            //let entries = self.omnipaxos_instances.lock().unwrap().read_entries(0..7);
            debug!("C{} LOG ENTRIES: {:?}", config_id, entries);
            debug!("C{} Leader = {:?}", config_id, instance.lock().unwrap().get_current_leader());
        }
    }
 
    pub(crate) async fn run(&mut self) { 
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        let mut displays_interval = time::interval(WAIT_LEADER_TIMEOUT);

        loop {
            tokio::select! {
                biased;

                _ = displays_interval.tick() => { self.debug_state(); },
                _ = election_interval.tick() => { self.handle_election_timeout(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.router.next() => { self.handle_incoming_msg(in_msg); },
                else => { }
            }
        }
        
    }    
}

