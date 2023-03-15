use omnipaxos_core::{
    messages::{Message, sequence_paxos::PaxosMessage},
    util::{NodeId, LogEntry}, 
    omni_paxos::{OmniPaxosConfig, OmniPaxos}};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex}, net::SocketAddr, clone,
};

use tokio::{sync::mpsc, time};
use tokio::net::TcpStream;
use futures::StreamExt;
use log::*;
use anyhow::{anyhow, Error};
use commitlog::LogOptions;
use sled::{Config};

use crate::{
    router::Router, 
    message::NodeMessage::{*, self}, 
    kv::{KeyValue, KVSnapshot},
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD, WAIT_LEADER_TIMEOUT},
};

pub struct OmniPaxosServer {
    address: SocketAddr,
    addresses: HashMap<NodeId, SocketAddr>,
    router: Router,
    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    configs: Vec<OmniPaxosConfig>,
}

type OmniPaxosKV = OmniPaxos<KeyValue, KVSnapshot, PersistentStorage<KeyValue, KVSnapshot>>;

impl OmniPaxosServer {
    pub async fn new(address: SocketAddr, addresses: HashMap<NodeId, SocketAddr>, config: OmniPaxosConfig) -> Self {
        let id = config.pid;

        let my_path = format!("my_db_{}", id);
        let my_logopts = LogOptions::new(my_path.clone());
        let my_sled_opts = Config::new().path(my_path.clone());

        // create configuration with given arguments
        let my_config = PersistentStorageConfig::with(my_path, my_logopts, my_sled_opts);
        
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =
            Arc::new(Mutex::new(config.clone().build(PersistentStorage::open(my_config))));
        omni_paxos.lock().unwrap().fail_recovery();

        let router: Router = Router::new(id, address, addresses.clone()).await.unwrap();
        OmniPaxosServer { address, addresses, router, omni_paxos, configs: vec![config] }
    }

    fn handle_incoming_msg(&mut self, in_msg: Result<NodeMessage, Error>) {
        debug!("Receiving: {:?}", in_msg);
        match in_msg {
            Ok(OmniPaxosMessage(msg)) => self.omni_paxos.lock().unwrap().handle_incoming(msg),
            Ok(Hello(id)) => {},//self.omni_paxos.lock().unwrap().reconnected(id), // TODO: gets called
                                                                              // on first connect
                                                                              // as well, is that
                                                                              // ok?
            Ok(Append(kv)) => {
                debug!("Got an append request from a client");
                self.omni_paxos.lock().unwrap().append(kv).expect("Failed on append");
            },
            Err(err) => warn!("Could not deserialize message: {}", err)
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            trace!("Sending to {}: {:?}", receiver, msg);
            match self.router.send_message(receiver, OmniPaxosMessage(msg)).await {
                Err(err) => warn!("Error sending message to node {}, {}", receiver, err),
                _ => ()
            }
        }
    }

    // TODO: still think there are race conditions with select! here (do the message handling
    // functions implement Drop?)
    pub(crate) async fn run(&mut self) { 
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        let mut displays_interval = time::interval(WAIT_LEADER_TIMEOUT);
        loop {
            tokio::select! {
                biased;

                _ = displays_interval.tick() => {
                    let mut entries: Vec<LogEntry<KeyValue, KVSnapshot>> = vec![];
                    let mut i = 0;
                    while let Some(entry) = self.omni_paxos.lock().unwrap().read(i) {
                        entries.push(entry);
                        i += 1;
                    }
                    //let entries = self.omni_paxos.lock().unwrap().read_entries(0..7);
                    debug!("LOG ENTRIES: {:?}", entries);
                },
                _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); debug!("Leader = {:?}", self.omni_paxos.lock().unwrap().get_current_leader()) },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.router.next() => { self.handle_incoming_msg(in_msg); },
                else => { }
            }
        }
    }    
}
