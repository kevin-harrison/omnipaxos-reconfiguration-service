// Log replication layer (copied from omnipaxos/example/kv_store) 
use crate::{
    kv::{KVSnapshot, KeyValue},
    server::OmniPaxosServer,
    util::*,
};
use omnipaxos_core::{
    messages::Message,
    omni_paxos::*,
    util::{LogEntry, NodeId},
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

mod kv;
mod server;
mod util;

type MessageKV = Message<KeyValue, KVSnapshot>;
type OmniPaxosKV = OmniPaxos<KeyValue, KVSnapshot, MemoryStorage<KeyValue, KVSnapshot>>;


// Service layer
// use uuid::Uuid;
use futures::prelude::*;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

mod router;
mod message;

#[tokio::main]
pub async fn main() {

    // parameter in testing env
    const SERVERS: [u64; 2] = [1, 2];
    let SERVERS_ADDR: HashMap<NodeId, String> = HashMap::<NodeId, String>::from([
            (1, "127.0.0.1:3000".to_string()),
            (2, "127.0.0.1:3001".to_string())
        ]);
    let configuration_id: u32 = 1;
    let pid: u64 = 1;

    // Build a OmniPaxosServer
    let peers = SERVERS.iter().filter(|&&p| p != pid).copied().collect();
    let config = OmniPaxosConfig {
        pid,
        configuration_id,
        peers,
        ..Default::default()
    };
    let omnipaxos: Arc<Mutex<OmniPaxosKV>> = 
            Arc::new(Mutex::new(config.build(MemoryStorage::default())));
    let mut omnipaxos_server = OmniPaxosServer {
        omnipaxos: Arc::clone(&omnipaxos),
        addresses: SERVERS_ADDR,
    };

    // Bind a server socket
    let listener = TcpListener::bind("127.0.0.1:17653").await.unwrap();

    println!("listening on {:?}", listener.local_addr());

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        // Delimit frames using a length header
        let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());

        // Deserialize frames
        let mut json_frame = tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalJson::<Value>::default(),
        );

        // Spawn a task that prints all received messages to STDOUT
        tokio::spawn(async move {
            while let Some(msg) = json_frame.try_next().await.unwrap() {
                println!("GOT: {:?}", msg);
                let p: Message<(), ()> = serde_json::from_value(msg).unwrap();
                println!("PARSED AS: {:?}", p)
            }
        });
    }
}
