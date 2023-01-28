//!
//! # Create TableFormat Request
//!
//! Converts TableFormat API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};

use tracing::{debug, info, trace, instrument};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{Status};
use fluvio_sc_schema::objects::{CommonCreateRequest};
use fluvio_sc_schema::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for tableformat request
#[instrument(skip(create, auth_ctx))]
pub async fn handle_create_tableformat_request<AC: AuthContext>(
    create: CommonCreateRequest,
    spec: TableFormatSpec,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    let name = create.name;

    info!(%name,"creating tableformat");

    if auth_ctx
        .global_ctx
        .tableformats()
        .store()
        .contains_key(&name)
        .await
    {
        debug!("tableformat already exists");
        return Ok(Status::new(
            name.to_string(),
            ErrorCode::TableFormatAlreadyExists,
            Some(format!("tableformat '{name}' already defined")),
        ));
    }

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(TableFormatSpec::OBJECT_TYPE, TypeAction::Create)
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

    let status = process_tableformat_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create tableformat response {:#?}", status);

    Ok(status)
}

/// Process custom tableformat, converts tableformat spec to K8 and sends to KV store
#[instrument(skip(ctx, name, tableformat_spec))]
async fn process_tableformat_request(
    ctx: &Context,
    name: String,
    tableformat_spec: TableFormatSpec,
) -> Status {
    if let Err(err) = ctx
        .tableformats()
        .create_spec(name.clone(), tableformat_spec)
        .await
    {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::TableFormatError, error) // TODO: create error type
    } else {
        info!(%name,"tableformat created");
        Status::new_ok(name.clone())
    }
}
