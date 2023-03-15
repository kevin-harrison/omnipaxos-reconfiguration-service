use futures::prelude::*;
use omnipaxos_core::util::NodeId;
use serde::{Serialize, Deserialize};
use std::{env, collections::HashMap, net::SocketAddr};

use tokio::net::TcpStream;
use tokio_serde::{formats::Cbor, Framed};
use tokio_util::codec::{Framed as CodecFramed, LengthDelimitedCodec};

mod message;
mod kv;
use crate::{
    message::NodeMessage,
    kv::KeyValue,
};

/*
#[derive(Clone, Debug, Serialize, Deserialize)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}
*/


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
    let node: NodeId = args[1].parse().expect("Couldn't parse node ID arg");
    let key = args[2].clone();
    let value = args[3].parse().expect("Couldn't parse value arg");

    // Create message
    let kv = KeyValue { 
        key,
        value,
    };

    // Bind a server socket
    let addresses = HashMap::<NodeId, SocketAddr>::from([(1, SocketAddr::from(([127,0,0,1], 8000))), (2, SocketAddr::from(([127,0,0,1], 8001))), (3, SocketAddr::from(([127,0,0,1], 8002)))]);
    let address = addresses.get(&node).unwrap();
    let tcp_stream = TcpStream::connect(address).await.expect("Couldn't connect to node"); 
    let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
    let mut framed: NodeConnection = Framed::new(length_delimited, Cbor::default());
    
    
    match framed.send(NodeMessage::Append(kv)).await {
        Ok(_) => println!("Message sent"),
        Err(err) => println!("Failed to end message: {}", err),
    }


}
