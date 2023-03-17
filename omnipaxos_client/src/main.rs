use futures::prelude::*;
use omnipaxos_core::{
    omni_paxos::ReconfigurationRequest,
    util::{ConfigurationId, NodeId},
};
use std::{collections::HashMap, env, net::SocketAddr};

use tokio::net::TcpStream;
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

mod kv;
mod message;
use crate::{
    kv::KeyValue,
    message::{
        log_migration::LogMigrationMsg::{self, *},
        NodeMessage::{self, *},
    },
};

type NodeConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NodeMessage,
    NodeMessage,
    Cbor<NodeMessage, NodeMessage>,
>;

#[tokio::main]
pub async fn main() {
    // Parse args
    let args: Vec<String> = env::args().collect();
    let command = args[1].clone();

    if command == "append" {
        let node: NodeId = args[2].parse().expect("Couldn't parse node ID arg");
        let config: ConfigurationId = args[3].parse().expect("Couldn't parse config ID arg");
        let key = args[4].clone();
        let value = args[5].parse().expect("Couldn't parse value arg");
        append(node, config, key, value).await;
    } else if command == "reconfig" {
        let node: NodeId = args[2].parse().expect("Couldn't parse node ID arg");
        let mut config_nodes: Vec<NodeId> = vec![];
        let mut i = 3;
        while let Some(id) = args.get(i) {
            config_nodes.push(id.parse().expect("Couldn't parse config node ID arg"));
            i += 1;
        }
        reconfigure(node, config_nodes).await;
    }
}

async fn append(node: NodeId, config: ConfigurationId, key: String, value: u64) {
    // Create message
    let kv = KeyValue { key, value };

    // Bind a server socket
    let addresses = HashMap::<NodeId, SocketAddr>::from([
        (1, SocketAddr::from(([127, 0, 0, 1], 8000))),
        (2, SocketAddr::from(([127, 0, 0, 1], 8001))),
        (3, SocketAddr::from(([127, 0, 0, 1], 8002))),
        (4, SocketAddr::from(([127, 0, 0, 1], 8003))),
        (5, SocketAddr::from(([127, 0, 0, 1], 8004))),
    ]);
    let address = addresses.get(&node).unwrap();
    let tcp_stream = TcpStream::connect(address)
        .await
        .expect("Couldn't connect to node");
    let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
    let mut framed: NodeConnection = Framed::new(length_delimited, Cbor::default());

    match framed.send(NodeMessage::Append(config, kv)).await {
        Ok(_) => println!("Message sent"),
        Err(err) => println!("Failed to end message: {}", err),
    }
}

async fn reconfigure(node: NodeId, nodes: Vec<NodeId>) {
    // Create message
    let request = ReconfigurationRequest::with(nodes, None);
    let message = LogMigrationMessage(StartNewConfiguration(request));

    // Bind a server socket
    let addresses = HashMap::<NodeId, SocketAddr>::from([
        (1, SocketAddr::from(([127, 0, 0, 1], 8000))),
        (2, SocketAddr::from(([127, 0, 0, 1], 8001))),
        (3, SocketAddr::from(([127, 0, 0, 1], 8002))),
        (4, SocketAddr::from(([127, 0, 0, 1], 8003))),
        (5, SocketAddr::from(([127, 0, 0, 1], 8004))),
    ]);
    let address = addresses.get(&node).unwrap();
    let tcp_stream = TcpStream::connect(address)
        .await
        .expect("Couldn't connect to node");
    let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
    let mut framed: NodeConnection = Framed::new(length_delimited, Cbor::default());

    match framed.send(message).await {
        Ok(_) => println!("Message sent"),
        Err(err) => println!("Failed to end message: {}", err),
    }
}
