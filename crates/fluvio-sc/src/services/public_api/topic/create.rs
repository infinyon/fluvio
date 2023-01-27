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

use fluvio_controlplane_metadata::topic::ReplicaSpec;
use fluvio_sc_schema::objects::CommonCreateRequest;
use fluvio_sc_schema::topic::validate::valid_topic_name;
use tracing::{info, debug, trace, instrument};

use fluvio_protocol::link::ErrorCode;

use fluvio_sc_schema::Status;
use fluvio_sc_schema::topic::TopicSpec;

use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::core::Context;
use crate::controllers::topics::generate_replica_map;
use crate::controllers::topics::update_replica_map_for_assigned_topic;
use crate::controllers::topics::validate_computed_topic_parameters;
use crate::controllers::topics::validate_assigned_topic_parameters;
use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
#[instrument(skip(create, auth_ctx))]
pub async fn handle_create_topics_request<AC: AuthContext>(
    create: CommonCreateRequest,
    topic: TopicSpec,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, IoError> {
    let name = create.name;

    info!( topic = %name,"creating topic");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(TopicSpec::OBJECT_TYPE, TypeAction::Create)
        .await
    {
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
    let mut status = validate_topic_request(&name, &topic, &auth_ctx.global_ctx).await;
    if status.is_error() {
        return Ok(status);
    }
    if !create.dry_run {
        status = process_topic_request(auth_ctx, name, topic).await;
    }

    trace!("create topics request response {:#?}", status);

    Ok(status)
}

/// Validate topic, takes advantage of the validation routines inside topic action workflow
async fn validate_topic_request(name: &str, topic_spec: &TopicSpec, metadata: &Context) -> Status {
    debug!("validating topic: {}", name);

    let valid_name = valid_topic_name(name);
    if !valid_name {
        return Status::new(
            name.to_string(),
            ErrorCode::TopicInvalidName,
            Some(format!("Invalid topic name: '{name}'. Topic name can contain only lowercase alphanumeric characters or '-'.")),
        );
    }

    let topics = metadata.topics().store();
    let spus = metadata.spus().store();
    // check if topic already exists
    if topics.contains_key(name).await {
        debug!("topic already exists");
        return Status::new(
            name.to_string(),
            ErrorCode::TopicAlreadyExists,
            Some(format!("Topic '{name}' already exists")),
        );
    }

    // check configuration
    if let Some(error) = topic_spec.validate_config() {
        return Status::new(
            name.to_string(),
            ErrorCode::TopicInvalidConfiguration,
            Some(error),
        );
    }

    match topic_spec.replicas() {
        ReplicaSpec::Computed(param) => {
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
        ReplicaSpec::Assigned(ref partition_map) => {
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

/// create new topic and wait until all partitions are fully provisioned
/// if any partitions are not provisioned in time, this will generate error
async fn process_topic_request<AC: AuthContext>(
    auth_ctx: &AuthServiceContext<AC>,
    name: String,
    topic_spec: TopicSpec,
) -> Status {
    use std::time::Duration;
    use once_cell::sync::Lazy;
    use tokio::select;
    use fluvio_future::timer::sleep;

    static MAX_WAIT_TIME: Lazy<u64> = Lazy::new(|| {
        use std::env;

        let var_value = env::var("FLV_TOPIC_WAIT").unwrap_or_default();
        let wait_time: u64 = var_value.parse().unwrap_or(90);
        wait_time
    });

    let topic_instance = match auth_ctx
        .global_ctx
        .topics()
        .create_spec(name.clone(), topic_spec)
        .await
    {
        Ok(instance) => instance,
        Err(err) => {
            return Status::new(
                name.clone(),
                ErrorCode::TopicNotProvisioned,
                Some(format!("error: {err}")),
            )
        }
    };

    let partition_count = topic_instance.spec.partitions();
    debug!(
        %partition_count,
        "waiting for partitions to be provisioned",

    );
    let topic_uid = &topic_instance.ctx().item().uid;

    let partition_ctx = auth_ctx.global_ctx.partitions();
    let mut partition_listener = partition_ctx.change_listener();
    let mut timer = sleep(Duration::from_secs(*MAX_WAIT_TIME));

    loop {
        let _ = partition_listener.sync_status_changes().await;

        let mut provisioned_count = 0;

        let read_guard = partition_ctx.store().read().await;
        // find partitions owned by topic which are online
        for partition in read_guard.values() {
            if partition.is_owned(topic_uid) && partition.status.is_online() {
                provisioned_count += 1;
                trace!(
                    "partition: {} online, total: {}",
                    partition.key(),
                    provisioned_count
                );
                if provisioned_count == partition_count {
                    info!(topic = %name, "Topic created successfully");
                    return Status::new_ok(name);
                }
            }
        }
        drop(read_guard);

        select! {
            _ = &mut timer  => {
                debug!("timer expired waiting for topic: {} provisioning",name);
                return Status::new(name, ErrorCode::TopicNotProvisioned, Some(format!("only {provisioned_count} out of {partition_count} provisioned")));
            },
            _ = partition_listener.listen() => {
                debug!("partition changed");
            }
        }
    }
}
