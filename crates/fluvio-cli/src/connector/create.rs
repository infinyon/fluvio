//!
//! # Create a Managed Connector
//!
//! CLI tree to generate Create a Managed Connector
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
pub struct CreateManagedConnectorOpt {
    /// The name for the new Managed Connector
    #[structopt(short = "c", long = "config", value_name = "config")]
    pub config: String,
}

impl CreateManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let config = ConnectorConfig::from_file(&self.config)?;
        let spec: ManagedConnectorSpec = config.clone().into();
        let name = spec.name.clone();

        debug!("creating managed_connector: {}, spec: {:#?}", name, spec);

        let admin = fluvio.admin().await;

        let replica_spec = ReplicaSpec::Computed(TopicReplicaParam::new(1, 1, false));
        debug!("topic spec: {:?}", replica_spec);
        match admin
            .create::<TopicSpec>(config.topic, false, replica_spec.into())
            .await
        {
            Err(FluvioError::AdminApi(ApiError::Code(ErrorCode::TopicAlreadyExists, _))) => {
                //println!("Topic already exists");
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }?;

        admin.create(name.to_string(), false, spec).await?;

        Ok(())
    }
}
