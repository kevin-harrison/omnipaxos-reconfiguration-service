use omnipaxos_core::{messages::{Message, sequence_paxos::PaxosMessage}, util::NodeId, omni_paxos::OmniPaxosConfig};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{sync::mpsc, time};
use tokio::net::TcpStream;
use futures::StreamExt;
use log::*;

use crate::{router::Router, message::NodeMessage::*};

pub struct OmniPaxosServer {
    //pub omni_paxos: Arc<Mutex<OmniPaxosKV>>,
    // pub incoming: mpsc::Receiver<Message<KeyValue, KVSnapshot>>,
    //pub outgoing: HashMap<NodeId, mpsc::Sender<Message<KeyValue, KVSnapshot>>>,
    addresses: HashMap<NodeId, String>,
    configs: Vec<OmniPaxosConfig>,

}

struct Data;

impl OmniPaxosServer {
    pub fn new(config: OmniPaxosConfig, addresses: HashMap<NodeId, String>) -> Self {
       OmniPaxosServer { addresses, configs: vec![config] }
    }

    
    pub(crate) async fn run(self) {
        


        let addr = "127.0.0.1:3000";
        let mut router: Router = Router::new(addr).await.unwrap();
        
        while let Some(node_msg) = router.next().await {
            match node_msg {
                Ok(OmniPaxosMessage(msg)) => (),
                Ok(Hello(id)) => (),
                Err(err) => {
                    warn!("Could not deserialize message:{}", err);
                }
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
    
}
