use futures::prelude::*;
use serde::{Serialize, Deserialize};
use std::env;

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
    // Create message
    let args: Vec<String> = env::args().collect();
    let kv = KeyValue { 
        key: args[1].clone(),
        value: args[2].parse().expect("Couldn't parse value arg"),
    };

    // Bind a server socket
    let tcp_stream = TcpStream::connect("127.0.0.1:8000").await.expect("Couldn't connect to node"); 
    let length_delimited = CodecFramed::new(tcp_stream, LengthDelimitedCodec::new());
    let mut framed: NodeConnection = Framed::new(length_delimited, Cbor::default());
    
    
    match framed.send(NodeMessage::Append(kv)).await {
        Ok(_) => println!("Message sent"),
        Err(err) => println!("Failed to end message: {}", err),
    }


}
