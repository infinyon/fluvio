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

use std::io::Error as IoError;

use tracing::{debug, trace};

use kf_protocol::api::FlvErrorCode;

use fluvio_sc_schema::FlvStatus;
use fluvio_sc_schema::topic::*;

use crate::core::*;
use crate::controllers::topics::generate_replica_map;
use crate::controllers::topics::update_replica_map_for_assigned_topic;
use crate::controllers::topics::validate_computed_topic_parameters;
use crate::controllers::topics::validate_assigned_topic_parameters;

/// Handler for create topic request
pub async fn handle_create_topics_request(
    name: String,
    dry_run: bool,
    topic_spec: TopicSpec,
    ctx: SharedContext,
) -> Result<FlvStatus, IoError> {
    debug!("api request: create topic '{}'", name);

    // validate topic request
    let mut status = validate_topic_request(&name, &topic_spec, &ctx).await;
    if !dry_run {
        status = process_topic_request(&ctx, name, topic_spec).await;
    }

    trace!("create topics request response {:#?}", status);

    Ok(status)
}

/// Validate topic, takes advantage of the validation routines inside topic action workflow
async fn validate_topic_request(
    name: &str,
    topic_spec: &TopicSpec,
    metadata: &Context,
) -> FlvStatus {
    debug!("validating topic: {}", name);

    let topics = metadata.topics().store();
    let spus = metadata.spus().store();
    // check if topic already exists
    if topics.contains_key(name).await {
        return FlvStatus::new(
            name.to_string(),
            FlvErrorCode::TopicAlreadyExists,
            Some(format!("topic '{}' already defined", name)),
        );
    }

    match topic_spec {
        TopicSpec::Computed(param) => {
            let next_state = validate_computed_topic_parameters(param);
            trace!("validating, computed topic: {:#?}", next_state);
            if next_state.resolution.is_invalid() {
                FlvStatus::new(
                    name.to_string(),
                    FlvErrorCode::TopicError,
                    Some(next_state.reason),
                )
            } else {
                let next_state = generate_replica_map(spus, param).await;
                trace!("validating, generate replica map topic: {:#?}", next_state);
                if next_state.resolution.no_resource() {
                    FlvStatus::new(
                        name.to_string(),
                        FlvErrorCode::TopicError,
                        Some(next_state.reason),
                    )
                } else {
                    FlvStatus::new_ok(name.to_owned())
                }
            }
        }
        TopicSpec::Assigned(ref partition_map) => {
            let next_state = validate_assigned_topic_parameters(partition_map);
            trace!("validating, computed topic: {:#?}", next_state);
            if next_state.resolution.is_invalid() {
                FlvStatus::new(
                    name.to_string(),
                    FlvErrorCode::TopicError,
                    Some(next_state.reason),
                )
            } else {
                let next_state = update_replica_map_for_assigned_topic(partition_map, spus).await;
                trace!("validating, assign replica map topic: {:#?}", next_state);
                if next_state.resolution.is_invalid() {
                    FlvStatus::new(
                        name.to_string(),
                        FlvErrorCode::TopicError,
                        Some(next_state.reason),
                    )
                } else {
                    FlvStatus::new_ok(name.to_owned())
                }
            }
        }
    }
}

/// Process topic, converts topic spec to K8 and sends to KV store
async fn process_topic_request(ctx: &Context, name: String, topic_spec: TopicSpec) -> FlvStatus {
    if let Err(err) = create_topic(ctx, name.clone(), topic_spec).await {
        let error = Some(err.to_string());
        FlvStatus::new(name, FlvErrorCode::TopicError, error)
    } else {
        FlvStatus::new_ok(name)
    }
}

async fn create_topic(ctx: &Context, name: String, topic: TopicSpec) -> Result<(), IoError> {
    ctx.topics().create_spec(name, topic).await.map(|_| ())
}
