//!
//! # Create SmartModule Request
//!
//! Converts SmartModule API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};

use tracing::{info, trace, debug, instrument};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{Status};
use fluvio_sc_schema::objects::{CommonCreateRequest};
use fluvio_sc_schema::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for smart module request
#[instrument(skip(create, auth_ctx))]
pub async fn handle_create_smartmodule_request<AC: AuthContext>(
    create: CommonCreateRequest,
    spec: SmartModuleSpec,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    let name = create.name;

    info!(%name,"creating smart module");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SmartModuleSpec::OBJECT_TYPE, TypeAction::Create)
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

    let status = process_smartmodule_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create smart module response {:#?}", status);

    Ok(status)
}

/// Process custom smart module, converts smart module spec to K8 and sends to KV store
#[instrument(skip(ctx, name, smartmodule_spec))]
async fn process_smartmodule_request(
    ctx: &Context,
    name: String,
    smartmodule_spec: SmartModuleSpec,
) -> Status {
    // if there is pkg associated with, we override name
    let store_id = if let Some(meta) = &smartmodule_spec.meta {
        if !meta.package.is_valid() {
            return Status::new(
                name,
                ErrorCode::SmartModuleError,
                Some("invalid SmartModule package".to_owned()),
            );
        }
        meta.store_id()
    } else {
        name
    };

    debug!(%store_id, "creating smart module");

    if let Err(err) = ctx
        .smartmodules()
        .create_spec(store_id.clone(), smartmodule_spec)
        .await
    {
        let error = Some(err.to_string());
        Status::new(store_id, ErrorCode::SmartModuleError, error) // TODO: create error type
    } else {
        info!(%store_id, "smart module created");
        Status::new_ok(store_id.clone())
    }
}
