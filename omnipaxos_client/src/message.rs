use serde::{Deserialize, Serialize};
use crate::kv::{KVSnapshot, KeyValue};
use omnipaxos_core::{
    util::{NodeId, ConfigurationId}, 
    messages::Message};


#[derive(Serialize, Deserialize, Debug)]
pub enum NodeMessage {
    Hello(NodeId),
    OmniPaxosMessage(ConfigurationId, Message<KeyValue, ()>),    
    Append(ConfigurationId, KeyValue),
}
