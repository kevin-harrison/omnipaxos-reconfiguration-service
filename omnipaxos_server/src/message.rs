use serde::{Deserialize, Serialize};
use crate::kv::{KVSnapshot, KeyValue};
use omnipaxos_core::{util::NodeId, messages::Message};

pub type ClientId = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum NodeMessage {
    Hello(NodeId),
    OmniPaxosMessage(Message<KeyValue, KVSnapshot>),
    
    // Reconfig transmission stuff
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    Hello(NodeId),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Hello(NodeId),
}
