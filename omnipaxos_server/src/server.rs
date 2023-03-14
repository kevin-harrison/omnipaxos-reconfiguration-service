use omnipaxos_core::{messages::{Message, sequence_paxos::PaxosMessage}, util::NodeId, omni_paxos::{OmniPaxosConfig, OmniPaxos}};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex}, net::SocketAddr,
};

use tokio::{sync::mpsc, time};
use tokio::net::TcpStream;
use futures::StreamExt;
use log::*;
use anyhow::{anyhow, Error};

use crate::{
    router::Router, 
    message::NodeMessage::{*, self}, 
    kv::{KeyValue, KVSnapshot},
    util::{ELECTION_TIMEOUT, OUTGOING_MESSAGE_PERIOD},
};

pub struct OmniPaxosServer {
    address: SocketAddr,
    addresses: HashMap<NodeId, SocketAddr>,
    router: Router,

    pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    // pub incoming: mpsc::Receiver<Message<KeyValue, KVSnapshot>>,
    //pub outgoing: HashMap<NodeId, mpsc::Sender<Message<KeyValue, KVSnapshot>>>,
    configs: Vec<OmniPaxosConfig>,

}

type OmniPaxosKV = OmniPaxos<KeyValue, KVSnapshot, MemoryStorage<KeyValue, KVSnapshot>>;

impl OmniPaxosServer {
    pub async fn new(address: SocketAddr, addresses: HashMap<NodeId, SocketAddr>, config: OmniPaxosConfig) -> Self {
        let omni_paxos: Arc<Mutex<OmniPaxosKV>> =
            Arc::new(Mutex::new(config.build(MemoryStorage::default())));
        let router: Router = Router::new(address, addresses.clone()).await.unwrap();
        OmniPaxosServer { address, addresses, router, omni_paxos, configs: vec![] }
    }

    async fn handle_incoming_msgs(&mut self, in_msg: Result<NodeMessage, Error>) {
        match in_msg {
            Ok(OmniPaxosMessage(msg)) => {
                self.omni_paxos.lock().unwrap().handle_incoming(msg);
            },
            Ok(Hello(_)) => (),
            Err(err) => {
                warn!("Could not deserialize message:{}", err);
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.lock().unwrap().outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            debug!("Sending to {}: {:?}", receiver, msg);
            match self.router.send_message(receiver, OmniPaxosMessage(msg)).await {
                Err(err) => error!("error sending message, {}", err),
                _ => ()
            }
        }
    }

    
    pub(crate) async fn run(&mut self) { 
        debug!("Starting loop");
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut election_interval = time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                biased;

                _ = election_interval.tick() => { self.omni_paxos.lock().unwrap().election_timeout(); },
                _ = outgoing_interval.tick() => { self.send_outgoing_msgs().await; },
                Some(in_msg) = self.router.next() => { self.handle_incoming_msgs(in_msg).await; },
                else => { }
            }
        }

        /*
        // sender / receiver - for data received from the server
        let (from_tcp_sr, from_tcp_rr) = mpsc::channel(100);

        // sender / receiver - for data to send to the server
        let (to_tcp_sr, to_tcp_rr) = mpsc::channel(100);

        // spawn a worker thread
        let work = tokio::spawn(OmniPaxosServer::process(to_tcp_sr, from_tcp_rr));

        // spawn a connector
        for node_id in self.addresses.keys() {
              
        }
        let tcp = tokio::spawn(OmniPaxosServer::connect(from_tcp_sr, to_tcp_rr, Arc::new(Mutex::new(self.addresses)), 2));

        // listen for an interruption
        tokio::signal::ctrl_c().await;
        // before exiting
        work.abort();
        tcp.abort();
        */
    }

    /*
    async fn process(sender_to_tcp: mpsc::Sender<Data>, 
                     receiver_from_tcp: mpsc::Receiver<Data>) {

        // use tokio::select!

        // and either forward to TCP the results of your async work
        // or get data from TCP and do something with it 

        // in a loop
    }

    async fn connect(sender_to_worker: mpsc::Sender<Data>, 
                     receiver_from_worker: mpsc::Receiver<Data>,
                     addresses: Arc<Mutex<HashMap<NodeId, String>>>,
                     id: NodeId) {

        let mut address = addresses.lock().unwrap().get(&id).unwrap().clone(); 
        loop {
            match TcpStream::connect(address).await { 
                Ok(stream) => {

                    // use tokio::select!

                    // and either get data from the stream
                    // forwarding it to worker, as needed

                    // or get data from the worker
                    // and forward it to the server

                    // on TCP connection issues, fall through
                    // to the timer for a reconnection attempt in 30 secs
                }
                Err(_) => ()
            }
            let secs_30 = core::time::Duration::from_secs(30);
            tokio::time::sleep(secs_30).await;
            if let Some(addr) = addresses.lock().unwrap().get(&id) {
                address = addr.clone();
            }
            else {break;}
        }
    }
    */
    
}
