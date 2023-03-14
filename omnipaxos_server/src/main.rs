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
    let args: Vec<String> = env::args().collect();
    let listen_address: SocketAddr = args[1].parse().expect("Unable to parse socket address");
    let addresses = HashMap::<NodeId, SocketAddr>::from([(1, SocketAddr::from(([127,0,0,1], 8000))), (2, SocketAddr::from(([127,0,0,1], 8001)))]);
    let id: NodeId = args[2].parse().expect("Unable to parse node ID");
    let peers = [1,2].iter().filter(|&&p| p != id).copied().collect();

    let config = OmniPaxosConfig {
        pid: id,
        configuration_id: 1,
        peers,
        ..Default::default()
    };
    let mut server = OmniPaxosServer::new(listen_address, addresses, config).await;
    server.run().await;

    /*
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
    */
}
