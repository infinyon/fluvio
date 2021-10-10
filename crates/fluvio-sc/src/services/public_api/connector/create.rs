//!
//! # Create Managed Connector Request
//!
//! Converts Managed Connector API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use dataplane::ErrorCode;
use fluvio_sc_schema::Status;
use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for managed connector request
pub async fn handle_create_managed_connector_request<AC: AuthContext>(
    name: String,
    spec: ManagedConnectorSpec,
    _dry_run: bool,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    debug!("creating managed connector: {}", name);

    let connector_count = auth_ctx
        .global_ctx
        .managed_connectors()
        .store()
        .read()
        .await
        .len();
    let max_connectors = auth_ctx.global_ctx.config().max_connectors;
    if connector_count >= max_connectors {
        return Err(Error::new(ErrorKind::Interrupted, format!("Max connectors reached. Must have less than {} connectors.", max_connectors)));
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
        Status::new_ok(name.clone())
    }
}
