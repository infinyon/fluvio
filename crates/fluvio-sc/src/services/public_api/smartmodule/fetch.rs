use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use fluvio_sc_schema::objects::{ListResponse, NameFilter, Metadata};
use fluvio_sc_schema::smartmodule::SmartModuleSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_request<AC: AuthContext>(
    filters: Vec<NameFilter>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse, Error> {
    trace!("fetching smart modules");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SmartModuleSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            debug!("fetch connector authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::SmartModule(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let connectors: Vec<Metadata<SmartModuleSpec>> = auth_ctx
        .global_ctx
        .smart_modules()
        .store()
        .read()
        .await
        .values()
        .filter_map(|value| {
            if filters.filter(value.key()) {
                Some(value.inner().clone().into())
            } else {
                None
            }
        })
        .collect();

    debug!("flv fetch connectors resp: {} items", connectors.len());
    trace!("flv fetch connectors resp {:#?}", connectors);

    Ok(ListResponse::SmartModule(connectors))
}
