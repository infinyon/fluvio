//!
//! # Update Topic Request
//!
//! Update topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a update message.
//!
use tracing::{trace, instrument};
use std::io::{Error, ErrorKind};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{
    topic::{AddPartition, MirrorConfig, ReplicaSpec, TopicResolution},
    Status,
};
use fluvio_stream_model::core::{MetadataItem, Spec};
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for udapte topic request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_add_partition<AC: AuthContext, C: MetadataItem>(
    topic_name: String,
    request: AddPartition,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status, Error> {
    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(TopicSpec::OBJECT_TYPE, InstanceAction::Update, &topic_name)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(Status::new(
                topic_name.clone(),
                ErrorCode::PermissionDenied,
                Some(String::from("permission denied")),
            ));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let topic = auth_ctx
        .global_ctx
        .topics()
        .store()
        .value(&topic_name)
        .await;

    if let Some(topic) = topic {
        let mut spec = topic.spec().clone();

        if matches!(
            &spec.replicas(),
            ReplicaSpec::Mirror(MirrorConfig::Remote(_))
        ) {
            return Ok(Status::new(
                topic_name,
                ErrorCode::MirrorUpdateFromRemote,
                Some("cannot update mirrored topic from remote".to_owned()),
            ));
        }

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
            ReplicaSpec::Assigned(_) => {
                return Ok(Status::new(
                    topic_name,
                    ErrorCode::TopicAssignmentReplicaCannotBeUpdated,
                    Some("assignment replica cannot be updated".to_owned()),
                ));
            }
            ReplicaSpec::Computed(replica_param) => {
                let mut new_replica_param = replica_param.clone();
                new_replica_param.partitions += request.number_of_partition;
                spec.set_replicas(ReplicaSpec::Computed(new_replica_param));
            }

            ReplicaSpec::Mirror(_) => {
                return Ok(Status::new(
                    topic_name,
                    ErrorCode::NotImplememented,
                    Some("add partition to a mirror topic is not implemented".to_owned()),
                ));
            }
        }

        let mut status = topic.status().clone();
        status.resolution = TopicResolution::Pending;

        auth_ctx
            .global_ctx
            .topics()
            .create_spec(topic.key.clone(), spec)
            .await?;

        auth_ctx
            .global_ctx
            .topics()
            .update_status(topic.key.clone(), status)
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
