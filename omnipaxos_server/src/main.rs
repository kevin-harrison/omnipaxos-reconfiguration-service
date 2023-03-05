use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::prelude::*;
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
    //kv::{KVSnapshot, KeyValue},
    server::OmniPaxosServer,
    //util::*,
};

mod server;
mod router;
mod kv;

#[tokio::main]
pub async fn main() {


    let config = OmniPaxosConfig {
        pid: 1,
        configuration_id: 1,
        peers: vec![2],
        ..Default::default()
    };
    let mut addresses = HashMap::<NodeId, String>::from([(1, "127.0.0.1:3000".to_string()), (2, "127.0.0.1:3001".to_string())]);
    let server = OmniPaxosServer::new(config, addresses);


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
