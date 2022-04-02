//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use tracing::debug;
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;
use crate::Result;

#[derive(Debug, Parser)]
pub struct DeleteTopicOpt {
    /// The name of the Topic to delete
    #[clap(value_name = "name")]
    topic: String,
}

impl DeleteTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        debug!("deleting topic: {}", &self.topic);
        let admin = fluvio.admin().await;
        admin.delete::<TopicSpec, _>(&self.topic).await?;
        println!("topic \"{}\" deleted", &self.topic);
        Ok(())
    }
}
