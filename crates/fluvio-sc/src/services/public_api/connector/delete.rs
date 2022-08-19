use std::io::{Error, ErrorKind};

use tracing::{info, trace, instrument};

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for delete managed connector request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_managed_connector<AC: AuthContext>(
    name: String,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    use fluvio_protocol::link::ErrorCode;

    info!(connector_name = %name, "deleting managed connector");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(
            ManagedConnectorSpec::OBJECT_TYPE,
            InstanceAction::Delete,
            &name,
        )
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
        .managed_connectors()
        .store()
        .value(&name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx
            .global_ctx
            .managed_connectors()
            .delete(name.clone())
            .await
        {
            Status::new(
                name.clone(),
                ErrorCode::ManagedConnectorError,
                Some(err.to_string()),
            )
        } else {
            info!(connector_name = %name, "managed connector deleted");
            Status::new_ok(name)
        }
    } else {
        Status::new(
            name,
            ErrorCode::ManagedConnectorNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete managed connector resp {:#?}", status);

    Ok(status)
}
