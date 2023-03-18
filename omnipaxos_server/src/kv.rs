use omnipaxos_core::storage::Snapshot;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            let KeyValue { key, value } = e;
            snapshotted.insert(key.clone(), *value);
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }    
}

impl KVSnapshot {

    pub fn get_chunk(self, chunk_index: usize, num_chunks: usize) -> Self {
        // Sort keys
        let mut v: Vec<_> = self.snapshotted.iter().collect();
        v.sort_by(|x,y| x.0.cmp(&y.0));

        // Calculate chunk bounds
        let quot = v.len() / num_chunks;
        let remainder = v.len() % num_chunks;
        let from = chunk_index * quot + std::cmp::min(chunk_index, remainder);
        let to = (chunk_index+1) * quot + std::cmp::min(chunk_index+1, remainder);
            
        // Create snapshot chunk
        let mut chunk = HashMap::<String, u64>::new();
        for (k, v) in v[from..to].iter() {
             chunk.insert((*k).clone(), **v);
        }
        Self { snapshotted: chunk }
    }
}
