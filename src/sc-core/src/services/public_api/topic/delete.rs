//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use tracing::{debug, trace};
use std::io::Error;

use kf_protocol::api::FlvErrorCode;
use sc_api::FlvStatus;

use crate::core::*;

/// Handler for delete topic request
pub async fn handle_delete_topic(
    topic_name: String,
    ctx: SharedContext,
) -> Result<FlvStatus, Error> {
    debug!("api request: delete topic '{}'", topic_name);

    let status = if ctx.topics().store().value(&topic_name).await.is_some() {
        if let Err(err) = ctx.topics().delete(topic_name.clone()).await {
            FlvStatus::new(
                topic_name.clone(),
                FlvErrorCode::TopicError,
                Some(err.to_string()),
            )
        } else {
            FlvStatus::new_ok(topic_name.clone())
        }
    } else {
        // topic does not exist
        FlvStatus::new(
            topic_name.clone(),
            FlvErrorCode::TopicNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(status)
}
