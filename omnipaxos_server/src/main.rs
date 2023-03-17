use env_logger;
use std::{collections::HashMap, env, net::SocketAddr, path::Path};

/*
use futures::prelude::*;
use router::Router;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
*/

use crate::{
    kv::{KVSnapshot, KeyValue},
    router::Router,
    server::OmniPaxosServer,
};
use hocon::HoconLoader;
use omnipaxos_core::{omni_paxos::OmniPaxosConfig, util::NodeId};
use omnipaxos_storage::persistent_storage::PersistentStorage;

mod kv;
mod message;
mod router;
mod server;
mod util;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    // Server config
    let args: Vec<String> = env::args().collect();
    let id: NodeId = args[1].parse().expect("Unable to parse node ID");

    // Addresses hardcoded for router
    let addresses = HashMap::<NodeId, SocketAddr>::from([
        (1, SocketAddr::from(([127, 0, 0, 1], 8000))),
        (2, SocketAddr::from(([127, 0, 0, 1], 8001))),
        (3, SocketAddr::from(([127, 0, 0, 1], 8002))),
        (4, SocketAddr::from(([127, 0, 0, 1], 8003))),
        (5, SocketAddr::from(([127, 0, 0, 1], 8004))),
    ]);

    let listen_address = addresses.get(&id).unwrap().clone();
    let router= Router::<KeyValue, KVSnapshot>::new(id, listen_address, addresses.clone())
        .await
        .unwrap();

    // Create OmniPaxos configs
    let mut configs: Vec<OmniPaxosConfig> = vec![];
    let config_dir = format!("config/node{}", id);
    for entry in Path::new(&config_dir)
        .read_dir()
        .expect("Config directory not found")
    {
        if let Ok(entry) = entry {
            let cfg = HoconLoader::new()
                .load_file(entry.path())
                .expect("Failed to load hocon file")
                .hocon()
                .unwrap();
            let config = OmniPaxosConfig::with_hocon(&cfg);
            configs.push(config);
        }
    }

    // Start server
    let mut server =
        OmniPaxosServer::<KeyValue, KVSnapshot, PersistentStorage<KeyValue, KVSnapshot>>::new(
            id, router, configs,
        );
    server.run().await;
}
