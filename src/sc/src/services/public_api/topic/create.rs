//!
//! # Create Topic Request
//!
//! Create topic request handler. There are 2 types of topics:
//!  * Topics with Computed Replicas (aka. Computed Topics)
//!  * Topics with Assigned Replicas (aka. Assigned Topics)
//!
//! Computed Topics use Fluvio algorithm for replica assignment.
//! Assigned Topics allow the users to apply their custom-defined replica assignment.
//!

use std::io::{Error as IoError, ErrorKind};

use tracing::{debug, trace};

use dataplane::ErrorCode;

use fluvio_sc_schema::Status;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_auth::{ AuthContext, TypeAction };
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::core::Context;
use crate::controllers::topics::generate_replica_map;
use crate::controllers::topics::update_replica_map_for_assigned_topic;
use crate::controllers::topics::validate_computed_topic_parameters;
use crate::controllers::topics::validate_assigned_topic_parameters;
use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
pub async fn handle_create_topics_request<AC: AuthContext>(
    name: String,
    dry_run: bool,
    topic_spec: TopicSpec,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, IoError> {
    debug!("api request: create topic '{}'", name);
    
    if let Ok(authorized) = auth_ctx.auth.allow_type_action(TopicSpec::OBJECT_TYPE, TypeAction::Create).await {
        if !authorized {
            trace!("authorization failed");
            return Ok(Status::new(
                name.clone(),
                ErrorCode::PermissionDenied,
                Some(String::from("permission denied")),
            ));
        }
    } else {
        return Err(IoError::new(
            ErrorKind::Interrupted,
            "authorization io error",
        ));
    }

    // validate topic request
    let mut status = validate_topic_request(&name, &topic_spec, &auth_ctx.global_ctx).await;
    if !dry_run {
        status = process_topic_request(auth_ctx, name, topic_spec).await;
    }

    trace!("create topics request response {:#?}", status);

    Ok(status)
}

/// Validate topic, takes advantage of the validation routines inside topic action workflow
async fn validate_topic_request(name: &str, topic_spec: &TopicSpec, metadata: &Context) -> Status {
    debug!("validating topic: {}", name);

    let topics = metadata.topics().store();
    let spus = metadata.spus().store();
    // check if topic already exists
    if topics.contains_key(name).await {
        return Status::new(
            name.to_string(),
            ErrorCode::TopicAlreadyExists,
            Some(format!("topic '{}' already defined", name)),
        );
    }

    match topic_spec {
        TopicSpec::Computed(param) => {
            let next_state = validate_computed_topic_parameters(param);
            trace!("validating, computed topic: {:#?}", next_state);
            if next_state.resolution.is_invalid() {
                Status::new(
                    name.to_string(),
                    ErrorCode::TopicError,
                    Some(next_state.reason),
                )
            } else {
                let next_state = generate_replica_map(spus, param).await;
                trace!("validating, generate replica map topic: {:#?}", next_state);
                if next_state.resolution.no_resource() {
                    Status::new(
                        name.to_string(),
                        ErrorCode::TopicError,
                        Some(next_state.reason),
                    )
                } else {
                    Status::new_ok(name.to_owned())
                }
            }
        }
        TopicSpec::Assigned(ref partition_map) => {
            let next_state = validate_assigned_topic_parameters(partition_map);
            trace!("validating, computed topic: {:#?}", next_state);
            if next_state.resolution.is_invalid() {
                Status::new(
                    name.to_string(),
                    ErrorCode::TopicError,
                    Some(next_state.reason),
                )
            } else {
                let next_state = update_replica_map_for_assigned_topic(partition_map, spus).await;
                trace!("validating, assign replica map topic: {:#?}", next_state);
                if next_state.resolution.is_invalid() {
                    Status::new(
                        name.to_string(),
                        ErrorCode::TopicError,
                        Some(next_state.reason),
                    )
                } else {
                    Status::new_ok(name.to_owned())
                }
            }
        }
    }
}

/// Process topic, converts topic spec to K8 and sends to KV store
async fn process_topic_request<AC: AuthContext>(
    auth_ctx: &AuthServiceContext<AC>,
    name: String,
    topic_spec: TopicSpec,
) -> Status {
    if let Err(err) = create_topic(auth_ctx, name.clone(), topic_spec).await {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::TopicError, error)
    } else {
        Status::new_ok(name)
    }
}

async fn create_topic<AC: AuthContext>(
    auth_ctx: &AuthServiceContext<AC>,
    name: String,
    topic: TopicSpec,
) -> Result<(), IoError> {
    auth_ctx
        .global_ctx
        .topics()
        .create_spec(name, topic)
        .await
        .map(|_| ())
}
