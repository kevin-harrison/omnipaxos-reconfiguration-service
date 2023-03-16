use serde::{Deserialize, Serialize};
use crate::kv::{KVSnapshot, KeyValue};
use omnipaxos_core::{util::NodeId, messages::Message};

use self::log_migration::LogMigrationMessage;

// pub type ClientId = u64;

pub mod log_migration {
    use omnipaxos_core::{
        storage::{Entry, Snapshot, SnapshotType, StopSign},
        util::NodeId,
    };
    use std::fmt::Debug;
    use serde::{Deserialize, Serialize};

    // The leader send this to inform new servers to start pulling logs from `des_servers`
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullStart {
        pub configuration_id: u32,
        pub des_servers: Vec<NodeId>,
        pub decided_idx: u64
    }

    // New servers ask for the log[idx_from..idx_to]
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullRequest {
        pub configuration_id: u32,
        pub idx_from: u64,
        pub idx_to: u64,
    }

    // Old servers respond to `LogPullRequest` from new servers
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullResponse<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        pub configuration_id: u32,
        pub idx_from: u64,
        pub idx_to: u64,
        pub logs: Vec<T>,
        pub snapshots: Option<SnapshotType<T, S>>,
    }

    // New servers tell the leader that they had finished syncing logs
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullDone {
        pub configuration_id: u32,
        pub get_idx: u64, // how many entries of log does the server get
    }

    // The leader tells all the servers to abondon OmniPaxos instances and start a new one.
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct StartNewConfiguration{
        pub configuration_id: u32,
    }
    
    #[allow(missing_docs)]
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum LogMigrationMsg<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        #[allow(missing_docs)]
        LogPullStart(LogPullStart),
        LogPullRequest(LogPullRequest),
        LogPullResponse(LogPullResponse<T, S>),
        LogPullDone(LogPullDone),
        StartNewConfiguration(StartNewConfiguration),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogMigrationMessage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        pub from: NodeId,
        pub to: NodeId,
        pub msg: LogMigrationMsg<T, S>,
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum NodeMessage {
    Hello(NodeId),
    OmniPaxosMessage(Message<KeyValue, KVSnapshot>),
    LogMigrationMessage(LogMigrationMessage<KeyValue, KVSnapshot>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerMessage {
    Hello(NodeId),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Hello(NodeId),
}
