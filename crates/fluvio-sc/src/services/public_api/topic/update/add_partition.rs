//!
//! # Add Partition Request
//!
use std::io::Error;

use tracing::instrument;

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{
    topic::{AddPartition, ReplicaSpec},
    Status,
};
use fluvio_stream_model::core::{MetadataItem, Spec};
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for add partition request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_add_partition<AC: AuthContext, C: MetadataItem>(
    topic_name: String,
    request: AddPartition,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status, Error> {
    let topic = auth_ctx
        .global_ctx
        .topics()
        .store()
        .value(&topic_name)
        .await;

    if let Some(topic) = topic {
        let mut spec = topic.spec().clone();

        if spec.is_system() {
            return Ok(Status::new(
                topic_name.clone(),
                ErrorCode::SystemSpecUpdatingAttempt {
                    kind: TopicSpec::LABEL.to_lowercase(),
                    name: topic_name,
                },
                None,
            ));
        };

        match spec.replicas() {
            ReplicaSpec::Computed(replica_param) => {
                let mut new_replica_param = replica_param.clone();
                new_replica_param.partitions += request.count;
                spec.set_replicas(ReplicaSpec::Computed(new_replica_param));
            }
            _ => {
                return Ok(Status::new(
                    topic_name,
                    ErrorCode::TopicInvalidReplicaType,
                    Some("invalid replica type".to_owned()),
                ));
            }
        }

        auth_ctx
            .global_ctx
            .topics()
            .create_spec(topic.key.clone(), spec)
            .await?;

        Ok(Status::new_ok(topic_name))
    } else {
        // topic does not exist
        Ok(Status::new(
            topic_name,
            ErrorCode::TopicNotFound,
            Some("not found".to_owned()),
        ))
    }
}
