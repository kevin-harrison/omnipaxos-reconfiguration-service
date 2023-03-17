use self::{client::ClientMessage, log_migration::LogMigrationMessage};

pub mod log_migration {
    use omnipaxos_core::{
        storage::{Entry, Snapshot, SnapshotType},
        util::{ConfigurationId, LogEntry, NodeId},
    };
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    // The leader send this to inform new servers to start pulling logs from `des_servers`
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullStart {
        pub des_servers: Vec<NodeId>,
        pub decided_idx: u64, // not include the `<StopSign>`
    }

    // New servers ask for the log[idx_from..idx_to)
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullRequest {
        pub from_idx: u64,
        pub to_idx: u64,
    }

    // Old servers respond to `LogPullRequest` from new servers
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullResponse<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        pub from_idx: u64,
        pub to_idx: u64,
        pub logs: Vec<LogEntry<T, S>>,
    }

    // New servers tell the leader that they had finished syncing logs
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct LogPullOneDone {
        pub get_idx: u64, // how many entries of log does the server get
    }

    // The leader tells all the servers to abondon OmniPaxos instances and start a new one.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct StartNewConfiguration {
        pub new_servers: Vec<NodeId>,
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
        LogPullOneDone(LogPullOneDone),
        StartNewConfiguration(StartNewConfiguration),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogMigrationMessage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        pub configuration_id: ConfigurationId,
        pub from: NodeId,
        pub to: NodeId,
        pub msg: LogMigrationMsg<T, S>,
    }

    impl<T, S> LogMigrationMessage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        pub fn get_receiver(&self) -> NodeId {
            self.to
        }
        pub fn get_sender(&self) -> NodeId {
            self.from
        }
    }
}

pub mod client {
    use omnipaxos_core::{
        omni_paxos::ReconfigurationRequest, storage::Entry, util::ConfigurationId,
    };
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Append<T>
    where
        T: Entry,
    {
        pub entry: T,
    }
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Reconfigure {
        pub request: ReconfigurationRequest,
    }
    pub enum ClientMsg<T>
    where
        T: Entry,
    {
        #[allow(missing_docs)]
        Append(Append<T>),
        Reconfigure(Reconfigure),
    }
    pub struct ClientMessage<T>
    where
        T: Entry,
    {
        pub configuration_id: ConfigurationId,
        pub msg: ClientMsg<T>,
    }
}

use omnipaxos_core::{
    messages::Message,
    storage::{Entry, Snapshot},
    util::{ConfigurationId, NodeId},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum NodeMessage<T, S>
where
    T: Entry,
    S: Snapshot<T>,
{
    // router messages
    Hello(NodeId),

    // server messages
    OmniPaxosMessage(ConfigurationId, Message<T, S>),
    LogMigrationMessage(LogMigrationMessage<T, S>),

    // client messages
    ClientMessage(ClientMessage<T>),
}
