//!
//! # Update a Managed Connector
//!
//! CLI tree to generate Update a Managed Connector
//!

use fluvio::metadata::topic::ReplicaSpec;
use structopt::StructOpt;
use tracing::debug;

use fluvio::{Fluvio, FluvioError};
use fluvio::metadata::{
    topic::{TopicSpec, TopicReplicaParam},
    connector::ManagedConnectorSpec,
};

use fluvio_sc_schema::ApiError;
use fluvio_sc_schema::errors::ErrorCode;

use crate::CliError;
use super::ConnectorConfig;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct UpdateManagedConnectorOpt {
    /// The configuration file for the update Managed Connector
    #[structopt(short = "c", long = "config", value_name = "config", required(true))]
    pub config: String,
}

impl UpdateManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let config = ConnectorConfig::from_file(&self.config)?;
        let spec: ManagedConnectorSpec = config.clone().into();
        let name = spec.name.clone();

        debug!(connector_name = %name, connector_spec = ?spec, "updating managed_connector");

        let admin = fluvio.admin().await;

        // admin.find ?
        let lists = admin.list::<ManagedConnectorSpec, _>(vec![]).await?;
        let existing_config = match lists.into_iter().find(|c| c.name == config.name) {
            None => {
                return Err(CliError::InvalidConnector(format!(
                    "{} - does not exist.",
                    config.name
                )))
            }
            Some(c) => c,
        };

        debug!(?existing_config, "Found connector");

        // topic already exists. original decision PR: 1823
        // A New Topic might have been defined - We will not delete the old one.
        // Check if the topic already exists by trying to create it
        let replica_spec = ReplicaSpec::Computed(TopicReplicaParam::new(1, 1, false));
        debug!(?replica_spec, "UpdateManagedConnectorOpt topic spec");
        match admin
            .create::<TopicSpec>(config.topic, false, replica_spec.into())
            .await
        {
            Err(FluvioError::AdminApi(ApiError::Code(ErrorCode::TopicAlreadyExists, _))) => Ok(()),
            Ok(_) => {
                debug!("UpdateManagedConnectorOpt - Created a new topic.");
                Ok(())
            }
            Err(e) => Err(e),
        }?;

        admin
            .delete::<ManagedConnectorSpec, _>(&config.name)
            .await?;
        admin.create(config.name, false, spec).await?;

        Ok(())
    }
}
