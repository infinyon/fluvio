//!
//! # Create a Managed Connector
//!
//! CLI tree to generate Create a Managed Connector
//!

use structopt::StructOpt;
use tracing::debug;

use fluvio::Fluvio;
use fluvio::metadata::topic::{TopicSpec, TopicReplicaParam};
use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;

use crate::cli::ClusterCliError;
use super::ConnectorConfig;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateManagedConnectorOpt {
    /// The name for the new Managed Connector
    #[structopt(short = "c", long = "config", value_name = "config")]
    pub config: String,
}

impl CreateManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ClusterCliError> {
        let config = ConnectorConfig::from_file(&self.config)?;
        let spec: ManagedConnectorSpec = config.clone().into();
        let name = spec.name.clone();

        debug!("creating managed_connector: {}, spec: {:#?}", name, spec);

        let admin = fluvio.admin().await;
        if config.create_topic {
            let topic_spec = TopicSpec::Computed(TopicReplicaParam::new(1, 1, false));
            debug!("topic spec: {:?}", topic_spec);
            admin.create(config.topic, false, topic_spec).await?;
        }
        admin.create(name.to_string(), false, spec).await?;

        Ok(())
    }
}
