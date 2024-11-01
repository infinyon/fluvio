use std::collections::BTreeMap;
use std::fmt;
use tracing::{debug, instrument};

use fluvio_controlplane_metadata::partition::PartitionMirrorConfig;
use fluvio_controlplane_metadata::partition::RemotePartitionConfig;
use fluvio_types::PartitionId;

use fluvio_controlplane::PartitionMetadata;
use fluvio_controlplane_metadata::topic::MirrorConfig;
use fluvio_controlplane_metadata::topic::MirrorMap;
use fluvio_controlplane_metadata::topic::ReplicaSpec;
use fluvio_controlplane_metadata::topic::TopicReplicaParam;
use fluvio_controlplane_metadata::topic::TopicResolution;

use fluvio_types::ReplicaMap;
use fluvio_controlplane_metadata::topic::PartitionMaps;
use fluvio_controlplane_metadata::topic::TopicStatus;
use fluvio_stream_model::core::MetadataItem;

use crate::controllers::scheduler::PartitionScheduler;
use crate::controllers::scheduler::ReplicaPartitionMap;
use crate::stores::spu::SpuLocalStore;
use crate::stores::spu::SpuLocalStorePolicy;
use crate::stores::topic::TopicMd;
use crate::stores::topic::TopicMetadata;

//
/// Validate assigned topic spec parameters and update topic status
///  * error is passed to the topic reason.
///
pub(crate) fn validate_assigned_topic_parameters<C: MetadataItem>(
    partition_map: &PartitionMaps,
) -> TopicNextState<C> {
    if let Err(err) = partition_map.validate() {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else {
        TopicStatus::next_resolution_pending().into()
    }
}

///
/// Validate computed topic spec parameters and update topic status
///  * error is passed to the topic reason.
///
pub(crate) fn validate_computed_topic_parameters<C: MetadataItem>(
    param: &TopicReplicaParam,
) -> TopicNextState<C> {
    if let Err(err) = ReplicaSpec::valid_partition(&param.partitions) {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else if let Err(err) = ReplicaSpec::valid_replication_factor(&param.replication_factor) {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else {
        TopicStatus::next_resolution_pending().into()
    }
}

///
/// Validate mirror topic spec parameters and update topic status
///  * error is passed to the topic reason.
///
pub(crate) fn validate_mirror_topic_parameter<C>(mirror: &MirrorConfig) -> TopicNextState<C>
where
    C: MetadataItem + Send + Sync,
{
    if let Err(err) = mirror.validate() {
        TopicStatus::next_resolution_invalid_config(err.to_string()).into()
    } else {
        TopicStatus::next_resolution_pending().into()
    }
}

///
/// Compare assigned SPUs versus local SPUs. If all assigned SPUs are live,
/// update topic status to ok. otherwise, mark as waiting for live SPUs
///
#[instrument(skip(partition_maps, spu_store))]
pub(crate) async fn update_replica_map_for_assigned_topic<C: MetadataItem>(
    partition_maps: &PartitionMaps,
    spu_store: &SpuLocalStore<C>,
) -> TopicNextState<C> {
    let partition_map_spus = partition_maps.unique_spus_in_partition_map();
    let spus_id = spu_store.spu_ids().await;

    // ensure spu exists
    for spu in &partition_map_spus {
        if !spus_id.contains(spu) {
            return TopicStatus::next_resolution_invalid_config(format!("invalid spu id: {spu}"))
                .into();
        }
    }

    let replica_map: ReplicaPartitionMap = partition_maps.into();
    if replica_map.is_empty() {
        TopicStatus::next_resolution_invalid_config("invalid replica map".to_owned()).into()
    } else {
        TopicNextState {
            resolution: TopicResolution::Provisioned,
            replica_map,
            ..Default::default()
        }
    }
}

/// values for next state
#[derive(Default, Debug)]
pub(crate) struct TopicNextState<C: MetadataItem> {
    pub resolution: TopicResolution,
    pub reason: String,
    pub replica_map: ReplicaPartitionMap,
    pub partitions: Vec<PartitionMetadata<C>>,
    pub mirror_map: MirrorMap,
}

impl<C: MetadataItem> fmt::Display for TopicNextState<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl<C: MetadataItem> From<(TopicResolution, String)> for TopicNextState<C> {
    fn from(val: (TopicResolution, String)) -> Self {
        let (resolution, reason) = val;
        Self {
            resolution,
            reason,
            ..Default::default()
        }
    }
}

impl<C: MetadataItem> From<((TopicResolution, String), ReplicaMap)> for TopicNextState<C> {
    fn from(val: ((TopicResolution, String), ReplicaMap)) -> Self {
        let ((resolution, reason), replica_map) = val;
        Self {
            resolution,
            reason,
            replica_map: replica_map.into(),
            ..Default::default()
        }
    }
}

impl<C: MetadataItem> From<((TopicResolution, String), Vec<PartitionMetadata<C>>)>
    for TopicNextState<C>
{
    fn from(val: ((TopicResolution, String), Vec<PartitionMetadata<C>>)) -> Self {
        let ((resolution, reason), partitions) = val;
        Self {
            resolution,
            reason,
            partitions,
            ..Default::default()
        }
    }
}

impl<C: MetadataItem> TopicNextState<C> {
    /// apply this state to topic and return set of partitions
    pub fn apply_as_next_state(self, topic: &mut TopicMetadata<C>) -> Vec<PartitionMetadata<C>> {
        topic.status.resolution = self.resolution;
        topic.status.reason = self.reason;
        if !self.replica_map.is_empty() {
            topic.status.set_replica_map(self.replica_map.into());
        }
        if !self.mirror_map.is_empty() {
            topic.status.set_mirror_map(self.mirror_map);
        }
        self.partitions
    }

    /// create same next state as given topic
    pub fn same_next_state(topic: &TopicMetadata<C>) -> TopicNextState<C> {
        TopicNextState {
            resolution: topic.status.resolution.clone(),
            ..Default::default()
        }
    }

    /// given topic, compute next state
    pub async fn compute_next_state<'a>(
        topic: &'a TopicMetadata<C>,
        scheduler: &'a mut PartitionScheduler<'a, C>,
    ) -> TopicNextState<C> {
        match topic.spec().replicas() {
            // Computed Topic
            ReplicaSpec::Computed(ref param) => match topic.status.resolution {
                TopicResolution::Init | TopicResolution::InvalidConfig => {
                    validate_computed_topic_parameters(param)
                }
                TopicResolution::Pending | TopicResolution::InsufficientResources => {
                    let replica_map = scheduler
                        .generate_replica_map_for_topic(
                            param,
                            Some(&topic.status().replica_map.clone().into()),
                        )
                        .await;
                    if replica_map.scheduled() {
                        debug!(
                            topic = %topic.key(),
                            "generated replica map for mirror topic"
                        );
                        TopicNextState {
                            resolution: TopicResolution::Provisioned,
                            replica_map,
                            ..Default::default()
                        }
                    } else {
                        TopicNextState {
                            resolution: TopicResolution::InsufficientResources,
                            ..Default::default()
                        }
                    }
                }
                _ => {
                    debug!(
                        topic = %topic.key(),
                        status = ?topic.status.resolution,
                        "partition generation status"
                    );
                    let mut next_state = TopicNextState::same_next_state(topic);
                    if next_state.resolution == TopicResolution::Provisioned {
                        debug!("creating new partitions");
                        next_state.partitions =
                            topic.create_new_partitions(scheduler.partitions()).await;
                    }
                    next_state
                }
            },

            // Assign Topic
            ReplicaSpec::Assigned(ref partition_map) => match topic.status.resolution {
                TopicResolution::Init | TopicResolution::InvalidConfig => {
                    validate_assigned_topic_parameters(partition_map)
                }
                TopicResolution::Pending | TopicResolution::InsufficientResources => {
                    let mut next_state =
                        update_replica_map_for_assigned_topic(partition_map, scheduler.spus())
                            .await;
                    if next_state.resolution == TopicResolution::Provisioned {
                        next_state.partitions =
                            topic.create_new_partitions(scheduler.partitions()).await;
                    }
                    next_state
                }
                _ => {
                    debug!(
                        "assigned topic: {} resolution: {:#?} ignoring",
                        topic.key, topic.status.resolution
                    );
                    let mut next_state = TopicNextState::same_next_state(topic);
                    if next_state.resolution == TopicResolution::Provisioned {
                        next_state.partitions =
                            topic.create_new_partitions(scheduler.partitions()).await;
                    }
                    next_state
                }
            },

            // Mirror Topic
            ReplicaSpec::Mirror(ref mirror_config) => match topic.status.resolution {
                // same logic for computed topic
                // should collapse
                TopicResolution::Init | TopicResolution::InvalidConfig => {
                    validate_mirror_topic_parameter(mirror_config)
                }
                TopicResolution::Pending | TopicResolution::InsufficientResources => {
                    let partitions = mirror_config.partition_count();
                    // create pseudo normal replica map
                    let replica_param = TopicReplicaParam {
                        partitions,
                        replication_factor: 1,
                        ..Default::default()
                    };

                    let replica_map = scheduler
                        .generate_replica_map_for_topic(
                            &replica_param,
                            Some(&topic.status().replica_map.clone().into()),
                        )
                        .await;

                    if replica_map.scheduled() {
                        debug!(
                            topic = %topic.key(),
                            "generated replica map for mirror topic"
                        );
                        let mut mirror_map = BTreeMap::new();

                        // generate mirror map
                        match mirror_config {
                            MirrorConfig::Remote(src) => {
                                for (partition, spu) in src.spus().iter().enumerate() {
                                    mirror_map.insert(
                                        partition as PartitionId,
                                        PartitionMirrorConfig::Remote(RemotePartitionConfig {
                                            home_spu_id: spu.id,
                                            home_spu_key: spu.key.clone(),
                                            home_cluster: src.home_cluster.clone(),
                                            home_spu_endpoint: spu.endpoint.clone(),
                                            target: src.target,
                                        }),
                                    );
                                }
                            }
                            MirrorConfig::Home(tgt) => {
                                for (partition, config) in tgt.partitions().iter().enumerate() {
                                    mirror_map.insert(
                                        partition as PartitionId,
                                        PartitionMirrorConfig::Home(config.clone()),
                                    );
                                }
                            }
                        }

                        TopicNextState {
                            resolution: TopicResolution::Provisioned,
                            mirror_map,
                            replica_map,
                            ..Default::default()
                        }
                    } else {
                        TopicNextState {
                            resolution: TopicResolution::InsufficientResources,
                            ..Default::default()
                        }
                    }
                }
                _ => {
                    debug!(
                        topic = %topic.key(),
                        status = ?topic.status.resolution,
                        "partition generation status"
                    );
                    let mut next_state = TopicNextState::same_next_state(topic);
                    if next_state.resolution == TopicResolution::Provisioned {
                        debug!("creating new partitions");
                        next_state.partitions =
                            topic.create_new_partitions(scheduler.partitions()).await;
                    }
                    next_state
                }
            },
        }
    }
}
