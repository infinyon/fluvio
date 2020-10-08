//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;
use crate::error::CliError;

#[derive(Debug, StructOpt)]
pub struct DeleteTopicOpt {
    #[structopt(value_name = "string")]
    topic: String,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete topic cli request
pub async fn process_delete_topic(
    fluvio: &Fluvio,
    opt: DeleteTopicOpt,
) -> Result<String, CliError> {
    debug!("deleting topic: {}", &opt.topic);
    let mut admin = fluvio.admin().await;
    admin.delete::<TopicSpec, _>(&opt.topic).await?;
    Ok(format!("topic \"{}\" deleted", &opt.topic))
}
