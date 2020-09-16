//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use tracing::{debug, trace};
use std::io::Error;

use dataplane_protocol::ErrorCode;
use fluvio_sc_schema::Status;

use crate::core::*;

/// Handler for delete topic request
pub async fn handle_delete_topic(
    topic_name: String,
    ctx: SharedContext,
) -> Result<Status, Error> {
    debug!("api request: delete topic '{}'", topic_name);

    let status = if ctx.topics().store().value(&topic_name).await.is_some() {
        if let Err(err) = ctx.topics().delete(topic_name.clone()).await {
            Status::new(
                topic_name.clone(),
                ErrorCode::TopicError,
                Some(err.to_string()),
            )
        } else {
            Status::new_ok(topic_name.clone())
        }
    } else {
        // topic does not exist
        Status::new(
            topic_name.clone(),
            ErrorCode::TopicNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(status)
}
