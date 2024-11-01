//!
//! # Add Mirror Request
//!
use std::io::Error;

use tracing::instrument;

use fluvio_protocol::{link::ErrorCode, record::ReplicaKey};
use fluvio_sc_schema::{
    mirror::MirrorType,
    partition::HomePartitionConfig,
    topic::{AddMirror, MirrorConfig, ReplicaSpec},
    Status,
};
use fluvio_stream_model::core::{MetadataItem, Spec};
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for add mirror request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_add_mirror<AC: AuthContext, C: MetadataItem>(
    topic_name: String,
    request: AddMirror,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status, Error> {
    let topic = auth_ctx
        .global_ctx
        .topics()
        .store()
        .value(&topic_name)
        .await;

    let remote = auth_ctx
        .global_ctx
        .mirrors()
        .store()
        .value(&request.remote_cluster)
        .await;

    if let Some(topic) = topic {
        match remote {
            None => {
                return Ok(Status::new(
                    topic_name,
                    ErrorCode::MirrorNotFound,
                    Some("not found".to_owned()),
                ));
            }
            Some(mirror) => match &mirror.spec().mirror_type {
                MirrorType::Remote(_) => {
                    match topic.spec().replicas() {
                        ReplicaSpec::Mirror(mc) => {
                            for (i, partition) in mc.as_partition_maps().maps().iter().enumerate() {
                                if let Some(partitions_mirror_config) = &partition.mirror {
                                    if let Some(home) = partitions_mirror_config.home() {
                                        if home.remote_cluster == request.remote_cluster {
                                            return Ok(Status::new(
                                                topic_name,
                                                ErrorCode::MirrorAlreadyExists,
                                                Some(format!("remote \"{}\" is already assigned to partition: \"{}\"", home.remote_cluster, i)),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            return Ok(Status::new(
                                topic_name,
                                ErrorCode::TopicError,
                                Some("invalid replica type".to_owned()),
                            ));
                        }
                    };
                }
                _ => {
                    return Ok(Status::new(
                        topic_name,
                        ErrorCode::MirrorInvalidType,
                        Some("remote cluster is not a remote mirror".to_owned()),
                    ));
                }
            },
        }

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
            ReplicaSpec::Mirror(MirrorConfig::Home(home_config)) => {
                let mut new_home_config = home_config.clone();
                let new_home_partition_config = HomePartitionConfig {
                    remote_cluster: request.remote_cluster,
                    remote_replica: { ReplicaKey::new(topic.key(), 0_u32).to_string() },
                    source: home_config.source,
                };
                new_home_config.add_partition(new_home_partition_config);
                spec.set_replicas(ReplicaSpec::Mirror(MirrorConfig::Home(new_home_config)));
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
            topic_name.clone(),
            ErrorCode::TopicNotFound,
            Some("not found".to_owned()),
        ))
    }
}
