use futures::prelude::*;
use serde::{Serialize, Deserialize};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

use omnipaxos_core::{
    omni_paxos::{OmniPaxos, OmniPaxosConfig},
};
use omnipaxos_storage::{
    memory_storage::MemoryStorage,
};

#[derive(Clone, Debug, Serialize, Deserialize)] // Clone and Debug are required traits.
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}


#[tokio::main]
pub async fn main() {

    // configuration with id 1 and the following cluster
    let configuration_id = 1;
    let cluster = vec![1, 2];

    // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on the other nodes)
    let my_pid = 2;
    let my_peers = vec![1];

    let omnipaxos_config = OmniPaxosConfig {
        configuration_id,
        pid: my_pid,
        peers: my_peers,
        ..Default::default()
    };

    let storage = MemoryStorage::<KeyValue, ()>::default();
    let mut omni_paxos = omnipaxos_config.build(storage);
   
    // doesn't add message to outgoing_messages because we haven't elected a leader yet;
    let write_entry = KeyValue { key: String::from("a"), value: 123 };
    omni_paxos.append(write_entry).expect("Failed to append");
    std::thread::sleep(std::time::Duration::from_millis(50));


    // Bind a server socket
    let socket = TcpStream::connect("127.0.0.1:17653").await.unwrap();

    // Delimit frames using a length header
    let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());

    // Serialize frames with JSON
    let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());
    
    // send outgoing messages. This should be called periodically, e.g. every ms
    for out_msg in omni_paxos.outgoing_messages() {
        //let receiver = out_msg.get_receiver();
        // send out_msg to receiver on network layer
        println!("{:?}", out_msg);

        // Send the value
        serialized
            .send(json!(out_msg))
            .await
            .unwrap();
    }
    

}
