//!
//! # Delete Custom Spus Request
//!
//! Lookup custom-spu in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use tracing::{debug, info, trace, instrument};
use std::io::{Error, ErrorKind};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::Status;
use fluvio_sc_schema::customspu::CustomSpuSpec;
use fluvio_controlplane_metadata::spu::CustomSpuKey;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::spu::store::SpuLocalStorePolicy;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::stores::spu::{SpuAdminMd};
use crate::services::auth::AuthServiceContext;

/// Handler for delete custom spu request
#[instrument(skip(key, auth_ctx))]
pub async fn handle_un_register_custom_spu_request<AC: AuthContext>(
    key: CustomSpuKey,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<Status, Error> {
    let spu_name = key.to_string();

    info!(%spu_name, "Deleting(unregistering) custom spu");
    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(
            CustomSpuSpec::OBJECT_TYPE,
            InstanceAction::Delete,
            &spu_name,
        )
        .await
    {
        if !authorized {
            trace!("authorization failed");
            let name: String = String::from(&key);
            return Ok(Status::new(
                name,
                ErrorCode::PermissionDenied,
                Some(String::from("permission denied")),
            ));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let spu_store = auth_ctx.global_ctx.spus().store();
    let status = match key {
        CustomSpuKey::Name(spu_name) => {
            debug!("api request: delete custom-spu with name '{}'", spu_name);

            // spu-name must exist
            if let Some(spu) = spu_store.value(&spu_name).await {
                un_register_custom_spu(auth_ctx, spu.inner_owned()).await
            } else {
                // spu does not exist
                Status::new(
                    spu_name.clone(),
                    ErrorCode::SpuNotFound,
                    Some("not found".to_owned()),
                )
            }
        }
        CustomSpuKey::Id(spu_id) => {
            debug!("api request: delete custom-spu with id '{}'", spu_id);

            // spu-id must exist
            if let Some(spu) = spu_store.get_by_id(spu_id).await {
                un_register_custom_spu(auth_ctx, spu).await
            } else {
                // spu does not exist
                Status::new(
                    format!("spu-{spu_id}"),
                    ErrorCode::SpuNotFound,
                    Some("not found".to_owned()),
                )
            }
        }
    };

    trace!("flv delete custom-spus resp {:#?}", status);

    Ok(status)
}

/// Generate for delete custom spu operation and return result.
async fn un_register_custom_spu<AC: AuthContext>(
    auth_ctx: &AuthServiceContext<AC>,
    spu: SpuAdminMd,
) -> Status {
    let spu_name = spu.key_owned();

    // must be Custom Spu
    if !spu.spec.is_custom() {
        return Status::new(
            spu_name,
            ErrorCode::SpuError,
            Some("expected 'Custom' spu, found 'Managed' spu".to_owned()),
        );
    }

    // delete custom spec and return result
    if let Err(err) = auth_ctx.global_ctx.spus().delete(spu_name.clone()).await {
        Status::new(
            spu_name,
            ErrorCode::SpuError,
            Some(format!("error deleting: {err}")),
        )
    } else {
        info!(%spu_name, "custom spu unregistered");
        Status::new_ok(spu_name)
    }
}
