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
    /// ignore delete errors if any
    #[clap(short, long, action, required = false)]
    ignore_error: bool,
    /// The name(s) of the Topic(s) to be deleted
    #[clap(value_name = "name", required = true)]
    names: Vec<String>,
}

impl DeleteTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        for topic in self.names.iter() {
            debug!("deleting topic: {}", topic);
            if let Err(error) = admin.delete::<TopicSpec, _>(topic).await {
                if self.ignore_error {
                    println!("topic \"{}\" delete failed with: {}", topic, error);
                } else {
                    return Err(error.into());
                }
            } else {
                println!("topic \"{}\" deleted", topic);
            }
        }
        Ok(())
    }
}
