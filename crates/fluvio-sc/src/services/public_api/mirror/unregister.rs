use fluvio_auth::AuthContext;
use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::{core::MetadataItem, Status};
use anyhow::Result;
use tracing::info;

use crate::services::auth::AuthServiceContext;

pub async fn handle_unregister_mirror<AC: AuthContext, C: MetadataItem>(
    key: String,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status> {
    let name = key;
    info!(name = name, "unregister mirror cluster");
    if auth_ctx.global_ctx.config().read_only_metadata {
        return Ok(Status::new(
            name.clone(),
            ErrorCode::Other("unable to change read-only configuration".to_owned()),
            Some(String::from("read-only error")),
        ));
    }

    let ctx = auth_ctx.global_ctx.clone();
    if (ctx.mirrors().store().value(&name).await).is_none() {
        return Ok(Status::new(
            name.clone(),
            ErrorCode::MirrorNotFound,
            Some(format!("remote cluster {:?} not found", name)),
        ));
    }

    if let Err(err) = ctx.mirrors().delete(name.clone()).await {
        return Ok(Status::new(
            name.clone(),
            ErrorCode::Other("remote to unregister remote cluster".to_owned()),
            Some(err.to_string()),
        ));
    }

    Ok(Status::new_ok(name))
}
