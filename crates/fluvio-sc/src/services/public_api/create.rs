use std::io::Error as IoError;

use tracing::instrument;

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status,ObjCreateRequest};
use fluvio_sc_schema::objects::{CreateRequest, AllCreatableSpec};
use fluvio_sc_schema::ObjectApiCreateRequest;
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
#[instrument(skip(request, auth_context))]
pub async fn handle_create_request<AC: AuthContext>(
    request: ObjCreateRequest,
    auth_context: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<Status>, IoError> {
    let (header, req) = request.get_header_request();

  //  let dry_run = req.dry_run;
  //  let name = req.name;
  //  tracing::debug!("Handling create request for {:?}", req.spec);

    let status = match req {
        ObjectApiCreateRequest::Topic(create) => {
            super::topic::handle_create_topics_request(create, auth_context).await?
        }
        ObjectApiCreateRequest::SpuGroup(create) => {
            super::spg::handle_create_spu_group_request(create, auth_context).await?
        }
        ObjectApiCreateRequest::CustomSpu(create) => {
            super::spu::RegisterCustomSpu::handle_register_custom_spu_request(
                create,
                auth_context,
            )
            .await
        }
        ObjectApiCreateRequest::ManagedConnector(create) => {
            super::connector::handle_create_managed_connector_request(
                create,
                auth_context,
            )
            .await?
        }
        ObjectApiCreateRequest::SmartModule(create) => {
            super::smartmodule::handle_create_smart_module_request(
                create,
                auth_context,
            )
            .await?
        }
        AllCreatableSpec::Table(spec) => {
            super::table::handle_create_table_request(name, spec, dry_run, auth_context).await?
        }
    };

    Ok(ResponseMessage::from_header(&header, status))
}
