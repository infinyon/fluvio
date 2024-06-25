//!
//! # Update Topic Request
//!
//! Update topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a update message.
//!

use anyhow::Result;
use tracing::{instrument, trace, debug, error};

use fluvio_protocol::link::ErrorCode;
use fluvio_stream_model::core::MetadataItem;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status, TryEncodableFrom};
use fluvio_sc_schema::objects::{ObjectApiUpdateRequest, UpdateRequest};
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for update topic request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_update_request<AC: AuthContext, C: MetadataItem>(
    request: RequestMessage<ObjectApiUpdateRequest>,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ResponseMessage<Status>> {
    let (header, del_req) = request.get_header_request();

    debug!(?del_req, "del request");

    let status = if let Some(req) = del_req.downcast()? as Option<UpdateRequest<TopicSpec>> {
        let action = req.action.clone();
        super::topic::update::handle_topic_update_request(req.key(), action, auth_ctx).await?
    } else {
        error!("unknown update request: {:#?}", del_req);
        Status::new(
            "update error".to_owned(),
            ErrorCode::Other("unknown admin object type".to_owned()),
            None,
        )
    };

    trace!("flv update topics resp {:#?}", status);

    Ok(ResponseMessage::from_header(&header, status))
}
