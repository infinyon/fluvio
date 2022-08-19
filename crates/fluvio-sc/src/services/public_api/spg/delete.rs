use std::io::{Error, ErrorKind};

use tracing::{info, trace, instrument};

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for delete spu group request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_spu_group<AC: AuthContext>(
    name: String,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    use fluvio_protocol::link::ErrorCode;

    info!(%name, "deleting spu group");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(SpuGroupSpec::OBJECT_TYPE, InstanceAction::Delete, &name)
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

    let status = if auth_ctx
        .global_ctx
        .spgs()
        .store()
        .value(&name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx.global_ctx.spgs().delete(name.clone()).await {
            Status::new(name.clone(), ErrorCode::SpuError, Some(err.to_string()))
        } else {
            info!(%name, "spu group deleted");
            Status::new_ok(name)
        }
    } else {
        Status::new(name, ErrorCode::SpuNotFound, Some("not found".to_owned()))
    };

    trace!("flv delete spu group resp {:#?}", status);

    Ok(status)
}
