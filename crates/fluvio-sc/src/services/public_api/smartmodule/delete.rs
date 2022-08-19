use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for delete smart module request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_smartmodule<AC: AuthContext>(
    name: String,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    use fluvio_protocol::link::ErrorCode;

    debug!("delete smart modules: {}", name);

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(SmartModuleSpec::OBJECT_TYPE, InstanceAction::Delete, &name)
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
        .smartmodules()
        .store()
        .value(&name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx
            .global_ctx
            .smartmodules()
            .delete(name.clone())
            .await
        {
            Status::new(
                name.clone(),
                ErrorCode::SmartModuleError,
                Some(err.to_string()),
            )
        } else {
            Status::new_ok(name)
        }
    } else {
        Status::new(
            name.clone(),
            ErrorCode::SmartModuleNotFound { name },
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete smart module resp {:#?}", status);

    Ok(status)
}
