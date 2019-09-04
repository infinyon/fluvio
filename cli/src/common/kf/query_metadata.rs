//!
//! # Kafka -- Query Metadata
//!
//! Communicates with Kafka Controller to retrieve Kafka Metadata for some or all topics
//!
use log::trace;

use kf_protocol::message::metadata::{KfMetadataRequest, KfMetadataResponse};
use kf_protocol::message::metadata::MetadataRequestTopic;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

// Query Kafka server for Brokers & Topic Metadata
pub async fn query_kf_metadata<'a>(
    conn: &'a mut Connection,
    topics: Option<Vec<String>>,
    versions: &'a KfApiVersions,
) -> Result<KfMetadataResponse, CliError> {
    let mut request = KfMetadataRequest::default();
    let version = kf_lookup_version(AllKfApiKey::Metadata, versions);

    // request topics metadata
    let request_topics = if let Some(topics) = topics {
        let mut req_topics: Vec<MetadataRequestTopic> = vec![];
        for name in topics {
            req_topics.push(MetadataRequestTopic { name });
        }
        Some(req_topics)
    } else {
        None
    };
    request.topics = request_topics;

    trace!("metadata req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("metadata res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
