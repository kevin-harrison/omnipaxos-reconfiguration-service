use omnipaxos_core::{
    omni_paxos::{OmniPaxos, OmniPaxosConfig},
    util::{ConfigurationId, NodeId},
};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
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
    message::{NodeMessage::{self, *}, log_migration::*},
    router::Router,
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD, WAIT_LEADER_TIMEOUT},
};

pub struct OmniPaxosServer {
    router: Router,
    omnipaxos_instances: HashMap<ConfigurationId, OmniPaxosInstance>,
    configs: HashMap<ConfigurationId, OmniPaxosConfig>,
    first_connects: HashMap<NodeId, bool>,
    active_instance: ConfigurationId
}

type OmniPaxosInstance = Arc<Mutex<OmniPaxos<KeyValue, KVSnapshot, PersistentStorage<KeyValue, KVSnapshot>>>>;

impl OmniPaxosServer {
    pub fn new(
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
            let omnipaxos_instance: OmniPaxosInstance = Arc::new(Mutex::new(
                config.clone().build(PersistentStorage::open(my_config)),
            ));
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
            router,
            omnipaxos_instances,
            configs: HashMap::new(), // config_map?
            first_connects: HashMap::new(),
            active_instance
        }
    }

    fn handle_incoming_msg(&mut self, in_msg: Result<NodeMessage, Error>) {
        trace!("Receiving: {:?}", in_msg);
        match in_msg {
            // Pass message to correct instance
            Ok(OmniPaxosMessage(cid, msg)) => {
                //if let omnipaxos_core::messages::Message::SequencePaxos(m) = msg.clone() {
                //    debug!("Receiving: {:?}", m);
                //}
                self
                .omnipaxos_instances
                .get(&cid)
                .unwrap()
                .lock()
                .unwrap()
                .handle_incoming(msg)
            },
            // On node reconnect call reconnect() on relevant instances
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
            Ok(LogMigrationMessage(logMigrationMessage)) => {
                self.handle_incoming_log_migration_msg(logMigrationMessage);
            }
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
            /*            
            Ok(Reconfig(rc)) => {
                debug!("Got an append request from a client");
                self.omnipaxos_instances
                    .get(&self.active_instance)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .reconfigure(rc)
                    .expect("Failed on append");
            }
            */
            Err(err) => warn!("Could not deserialize message: {}", err),
            
        }
    }

    fn handle_incoming_log_migration_msg(&mut self, logMigrationMessage: LogMigrationMessage) {
        let LogMigrationMessage(from, to, logMigrationMsg) = logMigrationMessage;
        match logMigrationMsg {
            LogPullStart(LogPullStart(cid, des_servers, decided_idx)) => {
                let cur_idx = 0;
                for des_server in des_servers {
                    if (cur_idx >= decided_idx) [
                        break;
                    ]
                    let batch_size = min(MIGRATTION_BATCH_SIZE, decided_idx-cur_idx)
                    match self
                        .router
                        .send_message(des_server, LogPullRequest(cid, cur_idx, cur_idx+batch_size))
                        .await {
                            Err(err) => trace!("Error sending LogPullRequest from {} to {}", self.NodeID, des_server)
                            _ => {

                            }
                        }
                    cur_idx += batch_size;
                }
            }
            LogPullRequest(LogPullRequest(cid, src_server, idx_from, idx_to)) => {
                // get KVs
                let logs = self.omnipaxos_instances
                    .get(&cid-1)
                    .unwrap()
                    .lock()
                    .unwrap()
                    .get()????
                let snapshots
                
                match self
                    .router
                    .send_message(src_server, LogPullResponse(cid, self.NodeID, idx_from, idx_to, logs, snapshots))
                    .await {
                        Err(err) => trace!("Error sending LogPullResponse from {} to {}", self.NodeID, des_server)
                        _ => {

                        }
                    }
            }
            LogPullResponse(LogPullResponse(cid, src_server, idx_from, idx_to, logs, snapshots)) => {
                for kv in logs {
                    self.omnipaxos_instances
                        .get(&cid)
                        .unwrap()
                        .lock()
                        .unwrap()
                        .append(kv)
                        .expect("Failed on append");
                }
            }
            LogPullDone(LogPullDone(cid, get_idx)) => {

            }
            StartNewConfiguration(StartNewConfiguration(cid)) => {

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
            //let entries = self.omnipaxos_instances.lock().unwrap().read_entries(0..7);
            debug!("C{} LOG ENTRIES: {:?}", config_id, entries);
            debug!(
                "C{} Leader = {:?}",
                config_id,
                instance.lock().unwrap().get_current_leader()
            );
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
