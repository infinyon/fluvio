//!
//! # Create Managed Connector Request
//!
//! Converts Managed Connector API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};

use tracing::{debug, info, trace, instrument};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::Status;
use fluvio_sc_schema::objects::CommonCreateRequest;
use fluvio_sc_schema::connector::ManagedConnectorSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for managed connector request
#[instrument(skip(create, auth_ctx))]
pub async fn handle_create_managed_connector_request<AC: AuthContext>(
    create: CommonCreateRequest,
    spec: ManagedConnectorSpec,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    let name = create.name;

    info!(connector_name = %name, "creating managed connector");

    if auth_ctx
        .global_ctx
        .managed_connectors()
        .store()
        .contains_key(&name)
        .await
    {
        debug!("connector already exists");
        return Ok(Status::new(
            name.to_string(),
            ErrorCode::ManagedConnectorAlreadyExists,
            Some(format!("connector '{}' already defined", name)),
        ));
    }

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(ManagedConnectorSpec::OBJECT_TYPE, TypeAction::Create)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(Status::new(
                name.clone(),
                ErrorCode::PermissionDenied,
                Some(String::from("permission denied")),
            ));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let status = process_managed_connector_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create managed connector response {:#?}", status);

    Ok(status)
}

/// Process custom managed connector, converts managed connector spec to K8 and sends to KV store
#[instrument(skip(ctx, name, managed_connector_spec))]
async fn process_managed_connector_request(
    ctx: &Context,
    name: String,
    managed_connector_spec: ManagedConnectorSpec,
) -> Status {
    if let Err(err) = ctx
        .managed_connectors()
        .create_spec(name.clone(), managed_connector_spec)
        .await
    {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::ManagedConnectorError, error) // TODO: create error type
    } else {
        info!(connector_name = %name, "managed connector created");
        Status::new_ok(name.clone())
    }
}
