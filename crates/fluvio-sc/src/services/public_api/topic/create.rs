//!
//! # Create Topic Request
//!
//! Create topic request handler. There are 3 types of topics:
//!  * Topics with Computed Replicas (aka. Computed Topics)
//!  * Topics with Assigned Replicas (aka. Assigned Topics)
//!  * Topics with Mirror Replicas (aka. Mirror Topics)
//!
//! Computed Topics use Fluvio algorithm for replica assignment.
//! Assigned Topics allow the users to apply their custom-defined replica assignment.
//! Mirror Topics are used for mirroring data from one topic to another.
//!

use tracing::{info, debug, trace, instrument};
use anyhow::{anyhow, Result};

use fluvio_protocol::link::ErrorCode;
use fluvio_controlplane_metadata::topic::ReplicaSpec;
use fluvio_sc_schema::objects::CreateRequest;
use fluvio_sc_schema::shared::validate_resource_name;
use fluvio_sc_schema::Status;
use fluvio_sc_schema::topic::TopicSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_controlplane_metadata::smartmodule::SmartModulePackageKey;
use fluvio_stream_model::core::MetadataItem;

use crate::controllers::topics::policy::{
    update_replica_map_for_assigned_topic, validate_assigned_topic_parameters,
    validate_computed_topic_parameters, validate_mirror_topic_parameter,
};
use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
#[instrument(skip(req, auth_ctx))]
pub(crate) async fn handle_create_topics_request<AC: AuthContext, C: MetadataItem>(
    req: CreateRequest<TopicSpec>,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status> {
    let (create, topic) = req.parts();
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
        return Err(anyhow!("authorization io error"));
    }

    // validate topic request
    let mut status = validate_topic_request::<C>(&name, &topic, &auth_ctx.global_ctx).await;
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
async fn validate_topic_request<C: MetadataItem>(
    name: &str,
    topic_spec: &TopicSpec,
    metadata: &Context<C>,
) -> Status {
    debug!("validating topic: {}", name);

    if let Err(err) = validate_resource_name(name) {
        return Status::new(
            name.to_string(),
            ErrorCode::TopicInvalidName,
            Some(format!("Invalid topic name: '{name}'. {err}")),
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

    // check if deduplication filter is present
    if let Some(deduplication) = topic_spec.get_deduplication() {
        let sm_name = deduplication.filter.transform.uses.as_str();
        let sm_fqdn = match SmartModulePackageKey::from_qualified_name(sm_name) {
            Ok(fqdn) => fqdn.store_id(),
            Err(err) => {
                return Status::new(
                    sm_name.to_string(),
                    ErrorCode::DeduplicationSmartModuleNameInvalid(err.to_string()),
                    Some(err.to_string()),
                )
            }
        };
        if !metadata.smartmodules().store().contains_key(&sm_fqdn).await {
            return Status::new(
                sm_name.to_string(),
                ErrorCode::DeduplicationSmartModuleNotLoaded,
                Some(format!(
                    "{}\nHint: try `fluvio hub download {sm_name}` and repeat this operation",
                    ErrorCode::DeduplicationSmartModuleNotLoaded
                )),
            );
        }
    }

    match topic_spec.replicas() {
        ReplicaSpec::Computed(param) => {
            let next_state = validate_computed_topic_parameters::<C>(param);
            trace!("validating, computed topic: {:#?}", next_state);
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
        ReplicaSpec::Assigned(ref partition_map) => {
            let next_state = validate_assigned_topic_parameters::<C>(partition_map);
            trace!("validating, computed topic: {:#?}", next_state);
            if next_state.resolution.is_invalid() {
                Status::new(
                    name.to_string(),
                    ErrorCode::TopicError,
                    Some(next_state.reason),
                )
            } else {
                let next_state =
                    update_replica_map_for_assigned_topic::<C>(partition_map, spus).await;
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
        ReplicaSpec::Mirror(ref mirror) => {
            let next_state = validate_mirror_topic_parameter::<C>(mirror);
            trace!("validating, mirror topic: {:#?}", next_state);
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

/// create new topic and wait until all partitions are fully provisioned
/// if any partitions are not provisioned in time, this will generate error
async fn process_topic_request<AC: AuthContext, C: MetadataItem>(
    auth_ctx: &AuthServiceContext<AC, C>,
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
        .create_spec(name.clone(), topic_spec.clone())
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

    // Mirror topics are not provisioned by the SC
    if let ReplicaSpec::Mirror(_) = topic_spec.replicas() {
        info!(topic = %name, "Topic created successfully");
        return Status::new_ok(name);
    }

    let partition_count = topic_instance.spec.partitions();
    debug!(
        %partition_count,
        "waiting for partitions to be provisioned",

    );
    let topic_uid = &topic_instance.ctx().item().uid();

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
