//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use log::{debug, trace};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};
use kf_protocol::api::FlvErrorCode;
use sc_api::{FlvResponseMessage};
use sc_api::topic::{FlvDeleteTopicsRequest, FlvDeleteTopicsResponse};
use k8_metadata::topic::TopicSpec as K8TopicSpec;

use super::PublicContext;

/// Handler for delete topic request
pub async fn handle_delete_topics_request(
    request: RequestMessage<FlvDeleteTopicsRequest>,
    ctx: &PublicContext,
) -> Result<ResponseMessage<FlvDeleteTopicsResponse>, Error> {
    let mut response = FlvDeleteTopicsResponse::default();
    let mut topic_results: Vec<FlvResponseMessage> = vec![];

    // process delete topic requests in sequence
    for topic_name in &request.request.topics {
        debug!("api request: delete topic '{}'", topic_name);

        // topic name must exist
        let result = if let Some(topic) = ctx.metadata().topics().topic(topic_name) {
            if let Some(item_ctx) = &topic.kv_ctx().item_ctx {
                let item = item_ctx.as_input();
                if let Err(err) = ctx.k8_client().delete_item::<K8TopicSpec,_>(&item).await {
                    FlvResponseMessage::new(
                        topic_name.clone(),
                        FlvErrorCode::TopicError,
                        Some(err.to_string()),
                    )
                } else {
                    FlvResponseMessage::new_ok(topic_name.clone())
                }
            } else {
                FlvResponseMessage::new_ok(topic_name.clone())
            }
        } else {
            // topic does not exist
            FlvResponseMessage::new(
                topic_name.clone(),
                FlvErrorCode::TopicNotFound,
                Some("not found".to_owned()),
            )
        };
        // push result
        topic_results.push(result);
    }

    // update response
    response.results = topic_results;
    trace!("flv delete topics resp {:#?}", response);

    Ok(request.new_response(response))
}
