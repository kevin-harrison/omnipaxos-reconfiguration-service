use crate::kv::{KVSnapshot, KeyValue};
use omnipaxos_core::{
    messages::Message,
    util::{ConfigurationId, NodeId},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum NodeMessage {
    Hello(NodeId),
    OmniPaxosMessage(ConfigurationId, Message<KeyValue, KVSnapshot>),
    Append(ConfigurationId, KeyValue),
}
