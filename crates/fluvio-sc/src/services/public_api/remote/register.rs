use fluvio_auth::AuthContext;
use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{core::MetadataItem, objects::CreateRequest, remote::RemoteSpec, Status};
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
    if auth_ctx.global_ctx.config().read_only_metadata {
        info!(req=?req, "change requested in read-only config");
        return Ok(Status::new(
            name.clone(),
            ErrorCode::Other("unable to change read-only configuration".to_owned()),
            Some(String::from("read-only error")),
        ));
    }
    let ctx = auth_ctx.global_ctx.clone();
    ctx.remote()
        .create_spec(name.clone(), spec)
        .await
        .map(|_| ())?;

    Ok(Status::new_ok(name))
}
