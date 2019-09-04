//!
//! # Fluvio SC - Query Topics
//!
//! Communicates with Fluvio Streaming Controller to retrieve Topics and convert
//! them to ScTopicMetadata
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use serde::Serialize;
use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::topic::FlvFetchTopicsRequest;
use sc_api::topic::FlvFetchTopicResponse;
use sc_api::topic::FlvFetchTopic;
use sc_api::topic::FlvTopicResolution;
use sc_api::topic::FlvTopicSpecMetadata;
use sc_api::topic::FlvPartitionReplica;
use sc_api::errors::FlvErrorCode;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;

// -----------------------------------
// ScTopicMetadata (Serializable)
// -----------------------------------

#[derive(Serialize, Debug)]
pub struct ScTopicMetadata {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<FlvErrorCode>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<Topic>,
}

#[derive(Serialize, Debug)]
pub struct Topic {
    pub type_computed: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_partitions: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<i32>,

    pub ignore_rack_assignment: bool,
    pub status: TopicResolution,
    pub reason: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_map: Option<Vec<PartitionReplica>>,
}

#[derive(Serialize, Debug)]
pub struct PartitionReplica {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub live_replicas: Vec<i32>,
}

#[derive(Serialize, Debug)]
pub enum TopicResolution {
    Provisioned,
    Init,
    Pending,
    InsufficientResources,
    InvalidConfig,
}

// -----------------------------------
// Implementation
// -----------------------------------
impl ScTopicMetadata {
    pub fn new(fetched_topic_metadata: &FlvFetchTopicResponse) -> Self {
        // if topic is present, convert it
        let topic = if let Some(fetched_topic) = &fetched_topic_metadata.topic {
            Some(Topic::new(fetched_topic))
        } else {
            None
        };

        // if error is present, convert it
        let error = if fetched_topic_metadata.error_code.is_error() {
            Some(fetched_topic_metadata.error_code)
        } else {
            None
        };

        // topic metadata with all parameters converted
        ScTopicMetadata {
            name: fetched_topic_metadata.name.clone(),
            error: error,
            topic: topic,
        }
    }
}

impl Topic {
    pub fn new(fetched_topic: &FlvFetchTopic) -> Self {
        let topic_resolution = TopicResolution::new(&fetched_topic.status.resolution);

        // convert partition replicas
        let partition_replicas =
            if let Some(flv_partition_replicas) = &fetched_topic.partition_replicas {
                let mut partition_replicas: Vec<PartitionReplica> = vec![];
                for flv_partition_replica in flv_partition_replicas {
                    partition_replicas.push(PartitionReplica::new(flv_partition_replica));
                }
                Some(partition_replicas)
            } else {
                None
            };

        // create Topic
        Topic {
            type_computed: fetched_topic.spec.is_computed(),
            assigned_partitions: fetched_topic.spec.partition_map_str(),
            partitions: fetched_topic.spec.partitions(),
            replication_factor: fetched_topic.spec.replication_factor(),
            ignore_rack_assignment: fetched_topic.spec.ignore_rack_assignment(),
            status: topic_resolution,
            reason: fetched_topic.status.reason_str().clone(),
            partition_map: partition_replicas,
        }
    }
    pub fn status_label(&self) -> &'static str {
        TopicResolution::resolution_label(&self.status)
    }

    pub fn type_label(&self) -> &'static str {
        FlvTopicSpecMetadata::type_label(&self.type_computed)
    }

    pub fn ignore_rack_assign_str(&self) -> &'static str {
        FlvTopicSpecMetadata::ignore_rack_assign_str(&self.ignore_rack_assignment)
    }

    pub fn partitions_str(&self) -> String {
        FlvTopicSpecMetadata::partitions_str(&self.partitions)
    }

    pub fn replication_factor_str(&self) -> String {
        FlvTopicSpecMetadata::replication_factor_str(&self.replication_factor)
    }
}

impl PartitionReplica {
    pub fn new(flv_partition_replica: &FlvPartitionReplica) -> Self {
        PartitionReplica {
            id: flv_partition_replica.id,
            leader: flv_partition_replica.leader,
            replicas: flv_partition_replica.replicas.clone(),
            live_replicas: flv_partition_replica.live_replicas.clone(),
        }
    }
}

impl TopicResolution {
    pub fn new(flv_topic_resolution: &FlvTopicResolution) -> Self {
        match flv_topic_resolution {
            FlvTopicResolution::Provisioned => TopicResolution::Provisioned,
            FlvTopicResolution::Init => TopicResolution::Init,
            FlvTopicResolution::Pending => TopicResolution::Pending,
            FlvTopicResolution::InsufficientResources => TopicResolution::InsufficientResources,
            FlvTopicResolution::InvalidConfig => TopicResolution::InvalidConfig,
        }
    }

    pub fn resolution_label(resolution: &TopicResolution) -> &'static str {
        match resolution {
            TopicResolution::Provisioned => "provisioned",
            TopicResolution::Init => "initializing",
            TopicResolution::Pending => "pending",
            TopicResolution::InsufficientResources => "no-resource-for-replica-map",
            TopicResolution::InvalidConfig => "invalid-config",
        }
    }
}

// -----------------------------------
// Query Server & Convert to Metadata
// -----------------------------------

/// Query Fluvio SC server for Topics and convert to Topic Metadata
pub fn query_sc_topic_metadata(
    server_addr: SocketAddr,
    names: Option<Vec<String>>,
) -> Result<Vec<ScTopicMetadata>, CliError> {
    match run_block_on(send_request_to_server(server_addr, names)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("cannot retrieve topics: {}", err),
        ))),
        Ok(fetched_topics) => {
            let mut topic_metadata_list: Vec<ScTopicMetadata> = vec![];
            for fetched_topic in &fetched_topics {
                topic_metadata_list.push(ScTopicMetadata::new(fetched_topic));
            }
            Ok(topic_metadata_list)
        }
    }
}

async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    names: Option<Vec<String>>,
) -> Result<Vec<FlvFetchTopicResponse>, CliError> {
    // look-up version
    let mut conn = Connection::new(&server_addr).await?;
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvFetchTopics, &versions);

    // generate request
    let mut request = FlvFetchTopicsRequest::default();
    request.names = names;

    trace!("query topic req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("query topic res '{}': {:#?}", server_addr, response);

    Ok(response.topics)
}
