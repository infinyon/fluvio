//!
//! # Create Spu Groups Request
//!
//! Converts Spu Gruups API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};

use tracing::{debug, trace};

use dataplane::ErrorCode;
use fluvio_sc_schema::Status;
use fluvio_service::auth::Authorization;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;

use crate::core::*;
use crate::services::auth::basic::{Action, Object};

/// Handler for spu groups request
pub async fn handle_create_spu_group_request(
    name: String,
    spec: SpuGroupSpec,
    _dry_run: bool,
    auth_ctx: &AuthenticatedContext,
) -> Result<Status, Error> {
    debug!("creating spu group: {}", name);
    let auth_request = (Action::Create, Object::SpuGroup, None);
    if let Ok(authorized) = auth_ctx.auth.enforce(auth_request).await {
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

    let status = process_custom_spu_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create spu-group response {:#?}", status);

    Ok(status)
}

/// Process custom spu, converts spu spec to K8 and sends to KV store
async fn process_custom_spu_request(ctx: &Context, name: String, spg_spec: SpuGroupSpec) -> Status {
    if let Err(err) = ctx.spgs().create_spec(name.clone(), spg_spec).await {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::SpuError, error)
    } else {
        Status::new_ok(name.clone())
    }
}
