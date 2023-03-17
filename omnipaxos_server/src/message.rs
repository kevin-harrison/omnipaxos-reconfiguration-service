use crate::kv::{KVSnapshot, KeyValue};

use self::log_migration::LogMigrationMessage;

pub mod log_migration {
    use omnipaxos_core::util::{ConfigurationId, LogEntry, NodeId};
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    use crate::kv::{KVSnapshot, KeyValue};

    // The leader send this to inform new servers to start pulling logs from `des_servers`
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct PullStart {
        pub des_servers: Vec<NodeId>,
        pub decided_idx: u64, // not include the `<StopSign>`
    }

    // New servers ask for the log[idx_from..idx_to)
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct PullRequest {
        pub from_idx: u64,
        pub to_idx: u64,
    }

    // Old servers respond to `LogPullRequest` from new servers
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct PullResponse {
        pub from_idx: u64,
        pub to_idx: u64,
        pub logs: Vec<LogEntry<KeyValue, KVSnapshot>>,
    }

    // New servers tell the leader that they had finished syncing logs
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct PullOneDone {
        pub get_idx: u64, // how many entries of log does the server get
    }

    // The leader tells all the servers to abondon OmniPaxos instances and start a new one.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct NewConfiguration {
        pub new_servers: Vec<NodeId>,
    }

    #[allow(missing_docs)]
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum LogMigrationMsg {
        #[allow(missing_docs)]
        LogPullStart(PullStart),
        LogPullRequest(PullRequest),
        LogPullResponse(PullResponse),
        LogPullOneDone(PullOneDone),
        StartNewConfiguration(NewConfiguration),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogMigrationMessage {
        pub configuration_id: ConfigurationId,
        pub from: NodeId,
        pub to: NodeId,
        pub msg: LogMigrationMsg,
    }

    impl LogMigrationMessage {
        pub fn get_receiver(&self) -> NodeId {
            self.to
        }
        pub fn get_sender(&self) -> NodeId {
            self.from
        }
    }
}

use omnipaxos_core::{
    messages::Message,
    omni_paxos::ReconfigurationRequest,
    util::{ConfigurationId, NodeId},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum NodeMessage {
    // router messages
    Hello(NodeId),

    // server messages
    OmniPaxosMessage(ConfigurationId, Message<KeyValue, KVSnapshot>),
    LogMigrateMessage(LogMigrationMessage),

    // client messages
    ClientMessage(ClientMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMsg {
    Append(KeyValue),
    Reconfigure(ReconfigurationRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientMessage {
    pub configuration_id: ConfigurationId,
    pub msg: ClientMsg,
}
