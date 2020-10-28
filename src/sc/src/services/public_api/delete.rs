//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use tracing::trace;
use std::io::Error;

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::Status;
use fluvio_sc_schema::objects::{DeleteRequest};
use fluvio_auth::{AuthContext};

use crate::services::auth::AuthServiceContext;

/// Handler for delete topic request
pub async fn handle_delete_request<AC: AuthContext>(
    request: RequestMessage<DeleteRequest>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<Status>, Error> {
    let (header, req) = request.get_header_request();

    let status = match req {
        DeleteRequest::Topic(name) => super::topic::handle_delete_topic(name, auth_ctx).await?,
        DeleteRequest::CustomSpu(key) => {
            super::spu::handle_un_register_custom_spu_request(key, auth_ctx).await?
        }
        DeleteRequest::SpuGroup(name) => {
            super::spg::handle_delete_spu_group(name, auth_ctx).await?
        }
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(ResponseMessage::from_header(&header, status))
}
