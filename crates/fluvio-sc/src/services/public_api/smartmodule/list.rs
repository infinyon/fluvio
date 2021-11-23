
use std::io::{Error, ErrorKind};

use tracing::{trace, debug, instrument};

use fluvio_sc_schema::objects::{ListResponse, Metadata};
use fluvio_sc_schema::smartmodule::SmartModuleMetadataSpec;
use fluvio_sc_schema::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_metadata_fetch_request<AC: AuthContext>(
    filters: Vec<String>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<SmartModuleMetadataSpec>, Error> {
    debug!("fetching custom spu list");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SmartModuleMetadataSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let smart_modules: Vec<Metadata<SmartModuleMetadataSpec>> = auth_ctx
        .global_ctx
        .smartmodules()
        .store()
        .read()
        .await
        .values()
        .filter_map(|value| {
            if filters.filter(value.key()) {
                Some(value.inner())
            } else {
                None
            }
        })
        .map(|value| {
            let spec : SmartModuleMetadataSpec = value.spec.clone().into();
            let status = value.status.clone();
            let spec : Metadata<SmartModuleMetadataSpec> = Metadata {
                name: value.key().to_string(),
                status,
                spec,
            };
            spec
        })
        .collect();

    debug!("flv fetch partitions resp: {} items", smart_modules.len());
    trace!("flv fetch partitions resp {:#?}", smart_modules);

    Ok(ListResponse::new(smart_modules))
}
