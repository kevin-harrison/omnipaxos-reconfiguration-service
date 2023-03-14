use std::{
    collections::HashMap,
    sync::{Arc, Mutex}, net::SocketAddr,
    env
};
use env_logger;


use futures::prelude::*;
use router::Router;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use omnipaxos_core::{
    messages::Message,
    omni_paxos::*,
    util::{LogEntry, NodeId},
};
use crate::{
    server::OmniPaxosServer,
};



mod server;
mod router;
mod kv;
mod message;
mod util;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    
    // Server config
    let args: Vec<String> = env::args().collect();
    let id: NodeId = args[1].parse().expect("Unable to parse node ID");
    let peers = [1,2].iter().filter(|&&p| p != id).copied().collect();
    let addresses = HashMap::<NodeId, SocketAddr>::from([(1, SocketAddr::from(([127,0,0,1], 8000))), (2, SocketAddr::from(([127,0,0,1], 8001))), (3, SocketAddr::from(([127,0,0,1], 8002)))]);
    let listen_address: SocketAddr = addresses.get(&id).unwrap().clone();
    let config = OmniPaxosConfig {
        pid: id,
        configuration_id: 1,
        peers,
        ..Default::default()
    };

    // Start server
    let mut server = OmniPaxosServer::new(listen_address, addresses, config).await;
    server.run().await;
}
