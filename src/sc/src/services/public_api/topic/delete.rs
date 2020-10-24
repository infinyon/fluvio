//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use tracing::{debug, trace};
use std::io::{Error, ErrorKind};

use dataplane::ErrorCode;
use fluvio_sc_schema::Status;

use crate::services::auth::AuthServiceContext;

/// Handler for delete topic request
pub async fn handle_delete_topic(
    topic_name: String,
    auth_ctx: &AuthServiceContext,
) -> Result<Status, Error> {
    debug!("api request: delete topic '{}'", topic_name);

    if let Ok(authorized) = auth_ctx.auth.delete(&topic_name).await {
        if !authorized {
            trace!("authorization failed");
            return Ok(Status::new(
                topic_name.clone(),
                ErrorCode::PermissionDenied,
                Some(String::from("permission denied")),
            ));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let status = if auth_ctx
        .global_ctx
        .topics()
        .store()
        .value(&topic_name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx
            .global_ctx
            .topics()
            .delete(topic_name.clone())
            .await
        {
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
