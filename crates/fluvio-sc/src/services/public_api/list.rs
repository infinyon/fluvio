use std::io::Error as IoError;

use tracing::{debug, instrument};

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{
    objects::{ObjectApiListRequest, ObjectApiListResponse},
};
use fluvio_auth::{AuthContext};

use crate::services::auth::AuthServiceContext;

#[instrument(skip(request, auth_ctx))]
pub async fn handle_list_request<AC: AuthContext>(
    request: RequestMessage<ObjectApiListRequest>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<ObjectApiListResponse>, IoError> {
    let (header, req) = request.get_header_request();
    debug!("list request: {:#?}", req);

    let response = match req {
        ObjectApiListRequest::Topic(req) => ObjectApiListResponse::Topic(
            super::topic::handle_fetch_topics_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::Spu(req) => ObjectApiListResponse::Spu(
            super::spu::handle_fetch_spus_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::SpuGroup(req) => ObjectApiListResponse::SpuGroup(
            super::spg::handle_fetch_spu_groups_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::CustomSpu(req) => ObjectApiListResponse::CustomSpu(
            super::spu::handle_fetch_custom_spu_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::Partition(req) => ObjectApiListResponse::Partition(
            super::partition::handle_fetch_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::ManagedConnector(req) => ObjectApiListResponse::ManagedConnector(
            super::connector::handle_fetch_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::SmartModule(req) => ObjectApiListResponse::SmartModule(
            super::smartmodule::handle_fetch_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::Table(req) => ObjectApiListResponse::Table(
            super::table::handle_fetch_request(req.name_filters, auth_ctx).await?,
        ),
        ObjectApiListRequest::SmartStream(req) => {
            //super::smartstream::handle_fetch_request(req.name_filters, auth_ctx).await?,
            todo!()
        }
    };

    debug!("response: {:#?}", response);

    Ok(ResponseMessage::from_header(&header, response))
}
