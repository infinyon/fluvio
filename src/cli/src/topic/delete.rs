//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;
use crate::Result;

#[derive(Debug, StructOpt)]
pub struct DeleteTopicOpt {
    /// The name of the Topic to delete
    #[structopt(value_name = "name")]
    topic: String,
}

impl DeleteTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        debug!("deleting topic: {}", &self.topic);
        let mut admin = fluvio.admin().await;
        admin.delete::<TopicSpec, _>(&self.topic).await?;
        println!("topic \"{}\" deleted", &self.topic);
        Ok(())
    }
}
