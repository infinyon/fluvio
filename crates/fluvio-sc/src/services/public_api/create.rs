use std::io::Error as IoError;

use tracing::{debug, instrument};

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status};
use fluvio_sc_schema::objects::{ObjectApiCreateRequest, ObjectCreateRequest};
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
#[instrument(skip(request, auth_context))]
pub async fn handle_create_request<AC: AuthContext>(
    request: RequestMessage<ObjectApiCreateRequest>,
    auth_context: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<Status>, IoError> {
    let (header, obj_req) = request.get_header_request();

    debug!("create request: {:#?}", obj_req);
    let ObjectApiCreateRequest { common, request } = obj_req;
    let status = match request {
        ObjectCreateRequest::Topic(create) => {
            super::topic::handle_create_topics_request(common, create, auth_context).await?
        }
        ObjectCreateRequest::SpuGroup(create) => {
            super::spg::handle_create_spu_group_request(common, create, auth_context).await?
        }
        ObjectCreateRequest::CustomSpu(create) => {
            super::spu::RegisterCustomSpu::handle_register_custom_spu_request(
                common,
                create,
                auth_context,
            )
            .await
        }
        ObjectCreateRequest::ManagedConnector(create) => {
            super::connector::handle_create_managed_connector_request(common, create, auth_context)
                .await?
        }
        ObjectCreateRequest::SmartModule(create) => {
            super::smartmodule::handle_create_smart_module_request(common, create, auth_context)
                .await?
        }
        ObjectCreateRequest::Table(create) => {
            super::table::handle_create_table_request(common, create, auth_context).await?
        }
    };

    Ok(ResponseMessage::from_header(&header, status))
}
