use fluvio_auth::AuthContext;
use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{
    core::MetadataItem,
    objects::CreateRequest,
    remote::{RemoteSpec, RemoteType},
    Status,
};
use anyhow::Result;
use tracing::info;

use crate::services::auth::AuthServiceContext;

pub async fn handle_register_remote<AC: AuthContext, C: MetadataItem>(
    req: CreateRequest<RemoteSpec>,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status> {
    let (create, spec) = req.clone().parts();
    let name = create.name;
    info!(name = name, "remote-cluster register");

    let ctx = auth_ctx.global_ctx.clone();

    if ctx.config().read_only_metadata {
        info!(req=?req, "change requested in read-only config");
        return Ok(Status::new(
            name.clone(),
            ErrorCode::Other("unable to change read-only configuration".to_owned()),
            Some(String::from("read-only error")),
        ));
    }

    // if it's a Edge, check if it already exists
    // if it's a Core, just update it
    if let Some(remote) = ctx.remote().store().value(&name).await {
        if let RemoteType::Edge(_) = remote.spec().remote_type {
            return Ok(Status::new(
                name.clone(),
                ErrorCode::RemoteAlreadyExists,
                Some(format!("remote cluster {:?} already exists", name)),
            ));
        }
    }

    ctx.remote()
        .create_spec(name.clone(), spec)
        .await
        .map(|_| ())?;

    Ok(Status::new_ok(name))
}
