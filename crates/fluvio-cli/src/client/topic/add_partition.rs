//!
//! # Increment partition of a Topic
//!
//! CLI tree to generate Delete Topics
//!
use clap::Parser;
use anyhow::Result;

use fluvio_sc_schema::topic::{AddPartition, TopicSpec, UpdateTopicAction};
use fluvio::Fluvio;

/// Option for Listing Partition
#[derive(Debug, Parser)]
pub struct AddPartitionOpt {
    /// Topic name
    topic: String,
    /// Number of Partitions
    #[arg(long, default_value = "1")]
    number_of_partition: i32,
}

impl AddPartitionOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;

        let request = AddPartition {
            number_of_partition: self.number_of_partition as u32,
        };

        let action = UpdateTopicAction::AddPartition(request);

        admin
            .update::<TopicSpec>(self.topic.clone(), action)
            .await?;

        println!("added partition to topic: {}", self.topic);

        Ok(())
    }
}
