//!
//! # Increment partition of a Topic
//!
//! CLI tree to generate Delete Topics
//!
use tracing::debug;
use clap::Parser;
use anyhow::Result;

use fluvio_sc_schema::topic::{AddPartition, ObjectTopicUpdateRequest, TopicUpdateRequest};
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
            topic: self.topic.clone(),
            number_of_partition: self.number_of_partition as u32,
        };

        let req = TopicUpdateRequest { request };

        debug!("sending connect request: {:#?}", req);

        let response = admin
            .send_receive_admin::<ObjectTopicUpdateRequest, _>(req)
            .await?;

        println!("response: {:#?}", response);

        //println!("connecting with \"{}\" cluster", home_id);
        Ok(())
    }
}
