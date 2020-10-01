//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use tracing::debug;
use structopt::StructOpt;

use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::topic::TopicSpec;
use crate::target::ClusterTarget;

#[derive(Debug, StructOpt)]
pub struct DeleteTopicOpt {
    #[structopt(value_name = "string")]
    topic: String,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl DeleteTopicOpt {
    /// Validate cli options. Generate target-server and delete-topic configuration.
    fn validate(self) -> eyre::Result<(FluvioConfig, String)> {
        let target_server = self.target.load()?;

        // return server separately from config
        Ok((target_server, self.topic))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete topic cli request
pub async fn process_delete_topic(opt: DeleteTopicOpt) -> eyre::Result<String> {
    let (target_server, name) = opt.validate()?;

    debug!("deleting topic: {}", name);

    let mut client = Fluvio::connect_with_config(&target_server).await?;
    let mut admin = client.admin().await;
    admin.delete::<TopicSpec, _>(&name).await?;
    Ok(format!("topic \"{}\" deleted", name))
}
