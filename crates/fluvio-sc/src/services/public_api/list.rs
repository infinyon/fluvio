use std::io::Error;
use tracing::{debug, instrument};

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::objects::{ListRequest, ListResponse};
use fluvio_auth::{AuthContext};

use crate::services::auth::AuthServiceContext;

#[instrument(skip(request, auth_ctx))]
pub async fn handle_list_request<AC: AuthContext>(
    request: RequestMessage<ListRequest>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<ListResponse>, Error> {
    debug!("handling list request");
    let (header, req) = request.get_header_request();

    let response = match req {
        ListRequest::Topic(filter) => {
            super::topic::handle_fetch_topics_request(filter, auth_ctx).await?
        }
        ListRequest::Spu(filter) => super::spu::handle_fetch_spus_request(filter, auth_ctx).await?,
        ListRequest::SpuGroup(filter) => {
            super::spg::handle_fetch_spu_groups_request(filter, auth_ctx).await?
        }
        ListRequest::CustomSpu(filter) => {
            super::spu::handle_fetch_custom_spu_request(filter, auth_ctx).await?
        }
        ListRequest::Partition(filter) => {
            super::partition::handle_fetch_request(filter, auth_ctx).await?
        }
        ListRequest::ManagedConnector(filter) => {
            super::connector::handle_fetch_request(filter, auth_ctx).await?
        }

        ListRequest::SmartModule(filter) => {
            super::smartmodule::handle_fetch_request(filter, auth_ctx).await?
        }
    };

    Ok(ResponseMessage::from_header(&header, response))
}
