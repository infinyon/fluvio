use std::io::{Error, ErrorKind};

use tracing::{info, trace, instrument};

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

/// Handler for delete tableformat request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_tableformat<AC: AuthContext>(
    name: String,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    use fluvio_protocol::link::ErrorCode;

    info!(%name, "deleting tableformat");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(TableFormatSpec::OBJECT_TYPE, InstanceAction::Delete, &name)
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
        .tableformats()
        .store()
        .value(&name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx
            .global_ctx
            .tableformats()
            .delete(name.clone())
            .await
        {
            Status::new(
                name.clone(),
                ErrorCode::TableFormatError,
                Some(err.to_string()),
            )
        } else {
            info!(%name, "tableformat deleted");
            Status::new_ok(name)
        }
    } else {
        Status::new(
            name,
            ErrorCode::TableFormatNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete tableformat resp {:#?}", status);

    Ok(status)
}
