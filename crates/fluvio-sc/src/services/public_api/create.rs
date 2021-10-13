use std::io::Error as IoError;

use tracing::instrument;

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::Status;
use fluvio_sc_schema::objects::{CreateRequest, AllCreatableSpec};
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
#[instrument(skip(request, auth_context))]
pub async fn handle_create_request<AC: AuthContext>(
    request: RequestMessage<CreateRequest>,
    auth_context: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<Status>, IoError> {
    let (header, req) = request.get_header_request();

    let dry_run = req.dry_run;
    let name = req.name;
    tracing::debug!("Handling create request for {:#?}", req.spec);

    let status = match req.spec {
        AllCreatableSpec::Topic(topic) => {
            super::topic::handle_create_topics_request(name, dry_run, topic, auth_context).await?
        }
        AllCreatableSpec::SpuGroup(group) => {
            super::spg::handle_create_spu_group_request(name, group, dry_run, auth_context).await?
        }
        AllCreatableSpec::CustomSpu(custom) => {
            super::spu::RegisterCustomSpu::handle_register_custom_spu_request(
                name,
                custom,
                dry_run,
                auth_context,
            )
            .await
        }
        AllCreatableSpec::ManagedConnector(spec) => {
            super::connector::handle_create_managed_connector_request(
                name,
                spec,
                dry_run,
                auth_context,
            )
            .await?
        }
        AllCreatableSpec::SmartModule(spec) => {
            super::smartmodule::handle_create_smart_module_request(
                name,
                spec,
                dry_run,
                auth_context,
            )
            .await?
        }
    };

    Ok(ResponseMessage::from_header(&header, status))
}
