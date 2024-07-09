//!
//! # Increment Remote to a Mirror Topic
//!
//! CLI tree to increment the number of partitions of a topic.
//!
use clap::Parser;
use anyhow::{anyhow, Result};

use fluvio_sc_schema::topic::{AddMirror, TopicSpec, UpdateTopicAction};
use fluvio::Fluvio;

/// Option for Listing Mirror
#[derive(Debug, Parser)]
pub struct AddMirrorOpt {
    /// Topic name
    topic: String,
    /// Remote cluster to add
    remote: String,
}

impl AddMirrorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;

        let _ = admin
            .list::<TopicSpec, _>(vec![self.topic.clone()])
            .await?
            .into_iter()
            .find(|t| t.name == self.topic)
            .ok_or_else(|| anyhow!("topic \"{}\" not found", self.topic))?;

        let request = AddMirror {
            remote_cluster: self.remote.clone(),
        };

        let action = UpdateTopicAction::AddMirror(request);
        admin
            .update::<TopicSpec>(self.topic.clone(), action.clone())
            .await?;

        println!("added new mirror to topic: \"{}\"", self.topic);

        Ok(())
    }
}
