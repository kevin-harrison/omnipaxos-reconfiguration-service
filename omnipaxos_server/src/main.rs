use std::{
    collections::HashMap,
    net::SocketAddr,
    env
};
use env_logger;

/*
use futures::prelude::*;
use router::Router;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
*/

use omnipaxos_core::{
    omni_paxos::*,
    util::NodeId,
};
use crate::server::OmniPaxosServer;



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
    let peers: Vec<NodeId> = [1,2,3].iter().filter(|&&p| p != id).copied().collect();
    let addresses = HashMap::<NodeId, SocketAddr>::from([(1, SocketAddr::from(([127,0,0,1], 8000))), (2, SocketAddr::from(([127,0,0,1], 8001))), (3, SocketAddr::from(([127,0,0,1], 8002)))]);
    
    let config = OmniPaxosConfig {
        pid: id,
        configuration_id: 1,
        peers: peers.clone(),
        logger_file_path: Some("logs".to_string()),
        ..Default::default()
    };

    let config2 = OmniPaxosConfig {
        pid: id,
        configuration_id: 2,
        peers,
        logger_file_path: Some("logs".to_string()),
        ..Default::default()
    };

    // Start server
    let mut server = OmniPaxosServer::new(addresses, vec![config, config2]).await;
    server.run().await;
}
