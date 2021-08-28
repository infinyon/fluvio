//!
//! # Create Spu Groups Request
//!
//! Converts Spu Gruups API request into KV request and sends to KV store for processing.
//!

use std::io::{Error, ErrorKind};
use std::time::Duration;

use fluvio_stream_dispatcher::actions::WSAction;
use tracing::{debug, trace, instrument};

use dataplane::ErrorCode;
use fluvio_sc_schema::Status;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

/// Handler for spu groups request
#[instrument(skip(name, spec, _dry_run, auth_ctx))]
pub async fn handle_create_spu_group_request<AC: AuthContext>(
    name: String,
    spec: SpuGroupSpec,
    _dry_run: bool,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    debug!("creating spu group: {}, replica: {}", name, spec.replicas);

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SpuGroupSpec::OBJECT_TYPE, TypeAction::Create)
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

    let status = process_custom_spu_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create spu-group response {:#?}", status);

    Ok(status)
}

/// Process custom spu, converts spu spec to K8 and sends to KV store
#[instrument(skip(ctx, name, spg_spec))]
async fn process_custom_spu_request(ctx: &Context, name: String, spg_spec: SpuGroupSpec) -> Status {
    if let Err(err) = ctx
        .spgs()
        .wait_action_with_timeout(
            &name,
            WSAction::UpdateSpec((name.clone(), spg_spec)),
            Duration::from_secs(120), // spg may take long to create
        )
        .await
    {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::SpuError, error)
    } else {
        Status::new_ok(name.clone())
    }
}
