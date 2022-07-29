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
                let error = CliError::from(error);
                err_happened = true;
                if self.continue_on_error {
                    let user_error = match error.get_user_error() {
                        Ok(usr_err) => usr_err.to_string(),
                        Err(err) => format!("{}", err),
                    };
                    println!("topic \"{}\" delete failed with: {}", name, user_error);
                } else {
                    return Err(error);
                }
            } else {
                println!("topic \"{}\" deleted", name);
            }
        }
        if err_happened {
            Err(CliError::CollectedError(
                "Failed deleting topic(s). Check previous errors.".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}
