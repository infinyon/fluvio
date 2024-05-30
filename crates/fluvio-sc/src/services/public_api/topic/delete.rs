//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use fluvio_stream_model::core::{MetadataItem, Spec};
use tracing::{info, trace, instrument};
use std::io::{Error, ErrorKind};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{
    topic::{MirrorConfig, ReplicaSpec},
    Status,
};
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for delete topic request
#[instrument(skip(topic_name, auth_ctx))]
pub async fn handle_delete_topic<AC: AuthContext, C: MetadataItem>(
    topic_name: String,
    force: bool,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status, Error> {
    info!(%topic_name, "Deleting topic");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(TopicSpec::OBJECT_TYPE, InstanceAction::Delete, &topic_name)
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

    let status = if let Some(topic) = auth_ctx
        .global_ctx
        .topics()
        .store()
        .value(&topic_name)
        .await
    {
        let spec = topic.spec();

        if matches!(
            &spec.replicas(),
            ReplicaSpec::Mirror(MirrorConfig::Remote(_))
        ) {
            return Ok(Status::new(
                topic_name,
                ErrorCode::MirrorDeleteFromRemote,
                Some("cannot delete mirrored topic from remote".to_owned()),
            ));
        }

        if !force && spec.is_system() {
            Status::new(
                topic_name.clone(),
                ErrorCode::SystemSpecDeletionAttempt {
                    kind: TopicSpec::LABEL.to_lowercase(),
                    name: topic_name,
                },
                None,
            )
        } else if let Err(err) = auth_ctx
            .global_ctx
            .topics()
            .delete(topic_name.clone())
            .await
        {
            Status::new(
                topic_name.clone(),
                ErrorCode::TopicError,
                Some(err.to_string()),
            )
        } else {
            info!(%topic_name, "topic deleted");
            Status::new_ok(topic_name)
        }
    } else {
        // topic does not exist
        Status::new(
            topic_name,
            ErrorCode::TopicNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(status)
}
