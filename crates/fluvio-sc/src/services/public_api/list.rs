use std::io::Error;
use tracing::{debug, instrument};

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::objects::{ListRequest, ListResponse};
use fluvio_sc_schema::{ObjListRequest, ObjListResponse, ObjectApiListRequest, ObjectApiListResponse};
use fluvio_auth::{AuthContext};

use crate::services::auth::AuthServiceContext;

#[instrument(skip(request, auth_ctx))]
pub async fn handle_list_request<AC: AuthContext>(
    request: ObjListRequest,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ObjListResponse, Error> {
    debug!("handling list request");
    let (header, obj, req) = request.get_header_request();

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
    };

    Ok(ObjListResponse::new(obj, response))
}
