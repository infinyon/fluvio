use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use fluvio_sc_schema::objects::{ListResponse, Metadata};
use fluvio_sc_schema::NameFilter;
use fluvio_sc_schema::connector::ManagedConnectorSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_request<AC: AuthContext>(
    filters: Vec<NameFilter>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<ManagedConnectorSpec>, Error> {
    trace!("fetching managed connectors");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(ManagedConnectorSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            debug!("fetch connector authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let connectors: Vec<Metadata<ManagedConnectorSpec>> = auth_ctx
        .global_ctx
        .managed_connectors()
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

    Ok(ListResponse::new(connectors))
}
