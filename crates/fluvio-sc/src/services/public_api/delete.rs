//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use tracing::{debug, instrument, trace};
use std::io::Error;

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status};
use fluvio_sc_schema::objects::{ObjectApiDeleteRequest};
use fluvio_auth::{AuthContext};

use crate::services::auth::AuthServiceContext;

/// Handler for delete topic request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_delete_request<AC: AuthContext>(
    request: RequestMessage<ObjectApiDeleteRequest>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<Status>, Error> {
    let (header, del_req) = request.get_header_request();

    debug!("del request: {:#?}", del_req);

    let status = match del_req {
        ObjectApiDeleteRequest::Topic(req) => {
            super::topic::handle_delete_topic(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::CustomSpu(req) => {
            super::spu::handle_un_register_custom_spu_request(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::SpuGroup(req) => {
            super::spg::handle_delete_spu_group(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::ManagedConnector(req) => {
            super::connector::handle_delete_managed_connector(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::SmartModule(req) => {
            super::smartmodule::handle_delete_smart_module(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::Table(req) => {
            super::table::handle_delete_table(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::SmartStream(_req) => {
            //super::table::handle_delete_table(req.key(), auth_ctx).await?
            todo!()
        }
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(ResponseMessage::from_header(&header, status))
}
