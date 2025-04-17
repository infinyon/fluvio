use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use fluvio::metadata::topic::TopicSpec;
use fluvio_sc_schema::topic::{ReplicaSpec, TopicReplicaParam};

#[derive(JsonSchema, Serialize, Deserialize)]
pub(crate) struct McpTopicConfig {
    partitions: u32,
    replication: u32,
}

impl TryInto<TopicSpec> for McpTopicConfig {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<TopicSpec> {
        let replica_spec = ReplicaSpec::Computed(TopicReplicaParam {
            partitions: self.partitions,
            replication_factor: self.replication,
            ..Default::default()
        });
        let spec: TopicSpec = replica_spec.into();
        Ok(spec)
    }
}
