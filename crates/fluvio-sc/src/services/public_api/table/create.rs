//!
//! # Create Table Request
//!
//! Converts Table API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use dataplane::ErrorCode;
use fluvio_sc_schema::{Status};
use fluvio_sc_schema::objects::CreateRequest;
use fluvio_sc_schema::table::TableSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for table request
#[instrument(skip(create, auth_ctx))]
pub async fn handle_create_table_request<AC: AuthContext>(
    create: CreateRequest<TableSpec>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    let name = create.name;
    let spec = create.spec.to_inner();

    debug!(%name,"creating table");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(TableSpec::OBJECT_TYPE, TypeAction::Create)
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

    let status = process_table_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create table response {:#?}", status);

    Ok(status)
}

/// Process custom table, converts table spec to K8 and sends to KV store
#[instrument(skip(ctx, name, table_spec))]
async fn process_table_request(ctx: &Context, name: String, table_spec: TableSpec) -> Status {
    if let Err(err) = ctx.tables().create_spec(name.clone(), table_spec).await {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::TableError, error) // TODO: create error type
    } else {
        Status::new_ok(name.clone())
    }
}
