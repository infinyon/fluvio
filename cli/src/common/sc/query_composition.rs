//!
//! # Kafka -- Query Topic Composition
//!
//! Query topic composition including replicas and SPUs
//!
use log::trace;

use sc_api::apis::ScApiKey;

use sc_api::topic::{FlvTopicCompositionRequest, FlvTopicCompositionResponse};
use sc_api::versions::ApiVersions;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_lookup_version;

/// Connect to server, get version, and for topic composition: Replicas and SPUs
pub async fn sc_get_topic_composition<'a>(
    conn: &'a mut Connection,
    topic: String,
    versions: &'a ApiVersions,
) -> Result<FlvTopicCompositionResponse, CliError> {
    let mut request = FlvTopicCompositionRequest::default();
    let version = sc_lookup_version(ScApiKey::FlvTopicComposition, &versions);

    request.topic_names = vec![topic];

    trace!(
        "topic composition req '{}': {:#?}",
        conn.server_addr(),
        request
    );

    let response = conn.send_request(request, version).await?;

    trace!(
        "topic composition res '{}': {:#?}",
        conn.server_addr(),
        response
    );

    Ok(response)
}
