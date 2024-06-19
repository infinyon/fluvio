//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use std::io::Read;

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::ApiError;
use tracing::debug;
use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::error::CliError;

#[derive(Debug, Parser)]
pub struct DeleteTopicOpt {
    /// Continue deleting in case of an error
    #[arg(short, long, required = false)]
    continue_on_error: bool,
    /// One or more name(s) of the topic(s) to be deleted
    #[arg(value_name = "name", required = true)]
    names: Vec<String>,
    /// Delete system topic(s)
    #[arg(short, long, required = false)]
    system: bool,
    /// Skip deletion confirmation
    #[arg(short, long, required = false)]
    force: bool,
}

impl DeleteTopicOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let mut err_happened = false;
        for name in self.names.iter() {
            debug!(name, "deleting topic");
            match admin.delete::<TopicSpec>(name).await {
                Err(error) if self.system && is_system_spec_error(&error) => {
                    if self.force || user_confirms(name) {
                        if let Err(err) = admin.force_delete::<TopicSpec>(name).await {
                            err_happened = true;
                            if self.continue_on_error {
                                println!("system topic \"{name}\" delete failed with: {err}");
                            } else {
                                return Err(error);
                            }
                        } else {
                            println!("system topic \"{name}\" deleted");
                        }
                    } else {
                        println!("Aborted");
                        break;
                    }
                }
                Err(error) => {
                    err_happened = true;
                    if self.continue_on_error {
                        println!("topic \"{name}\" delete failed with: {error}");
                    } else {
                        return Err(error);
                    }
                }

                Ok(_) => {
                    println!("topic \"{name}\" deleted");
                }
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

fn is_system_spec_error(error: &anyhow::Error) -> bool {
    matches!(
        error.root_cause().downcast_ref::<ApiError>(),
        Some(ApiError::Code(
            ErrorCode::SystemSpecDeletionAttempt {
                kind: _kind,
                name: _name,
            },
            None
        ))
    )
}

fn user_confirms(name: &str) -> bool {
    println!("You are trying to delete a system topic '{name}'. It can affect the functioning of the cluster.
                             \nAre you sure you want to proceed? (y/n)");
    char::from(
        std::io::stdin()
            .bytes()
            .next()
            .and_then(|b| b.ok())
            .unwrap_or_default(),
    ) == 'y'
}
