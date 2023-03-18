use env_logger;
use std::{collections::HashMap, env, net::SocketAddr, path::Path};
use hocon::HoconLoader;
use omnipaxos_core::{omni_paxos::OmniPaxosConfig, util::NodeId};
use crate::{
    router::Router,
    server::OmniPaxosServer,
};

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
    let router = Router::new(id, listen_address, addresses.clone())
        .await
        .unwrap();

    // Create OmniPaxos configs
    let mut configs: Vec<OmniPaxosConfig> = vec![];
    let config_dir = format!("config/node{}", id);
    if let Ok(config_files) = Path::new(&config_dir).read_dir() {
        for entry in config_files {
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
    }

    // Start server
    let mut server = OmniPaxosServer::new(id, router, configs);
    server.run().await;
}
