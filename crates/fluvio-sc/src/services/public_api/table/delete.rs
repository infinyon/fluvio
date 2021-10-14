use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::table::TableSpec;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for delete table request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_table<AC: AuthContext>(
    name: String,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    use dataplane::ErrorCode;

    debug!("delete tables: {}", name);

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(TableSpec::OBJECT_TYPE, InstanceAction::Delete, &name)
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
        .tables()
        .store()
        .value(&name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx.global_ctx.tables().delete(name.clone()).await {
            Status::new(name.clone(), ErrorCode::TableError, Some(err.to_string()))
        } else {
            Status::new_ok(name)
        }
    } else {
        Status::new(name, ErrorCode::TableNotFound, Some("not found".to_owned()))
    };

    trace!("flv delete table resp {:#?}", status);

    Ok(status)
}
