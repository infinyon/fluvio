use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use fluvio_sc_schema::objects::{ListResponse, NameFilter, Metadata};
use fluvio_sc_schema::derivedstream::DerivedStreamSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_request<AC: AuthContext>(
    filters: Vec<NameFilter>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<SmartModuleSpec>, Error> {
    trace!("fetching smartmodules");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SmartModuleSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            debug!("fetch smartmodule authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let smartmodules: Vec<Metadata<SmartModuleSpec>> = auth_ctx
        .global_ctx
        .smartmodules()
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

    debug!("flv fetch smartmodules resp: {} items", smartmodules.len());
    trace!("flv fetch smartmodules resp {:#?}", smartmodules);

    Ok(ListResponse::new(smartmodules))
}
