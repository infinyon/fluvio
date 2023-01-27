//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use tracing::debug;
use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::error::CliError;

#[derive(Debug, Parser)]
pub struct DeleteTopicOpt {
    /// Continue deleting in case of an error
    #[clap(short, long, action, required = false)]
    continue_on_error: bool,
    /// One or more name(s) of the topic(s) to be deleted
    #[clap(value_name = "name", required = true)]
    names: Vec<String>,
}

impl DeleteTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let mut err_happened = false;
        for name in self.names.iter() {
            debug!(name, "deleting topic");
            if let Err(error) = admin.delete::<TopicSpec, _>(name).await {
                err_happened = true;
                if self.continue_on_error {
                    println!("topic \"{name}\" delete failed with: {error}");
                } else {
                    return Err(error);
                }
            } else {
                println!("topic \"{name}\" deleted");
            }
        }
        if err_happened {
            Err(CliError::CollectedError(
                "Failed deleting topic(s). Check previous errors.".to_string(),
            )
            .into())
        } else {
            Ok(())
        }
    }
}
