use std::io::{Error, ErrorKind};

use fluvio_controlplane_metadata::core::Spec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_sc_schema::AdminSpec;
use fluvio_stream_dispatcher::store::StoreContext;
use tracing::{debug, trace, instrument};

use fluvio_sc_schema::objects::{ListResponse, NameFilter, Metadata};
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::store::{KeyFilter};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_request<AC: AuthContext>(
    filters: Vec<NameFilter>,
    auth_ctx: &AuthServiceContext<AC>,
    object_ctx: &StoreContext<SmartModuleSpec>,
) -> Result<ListResponse<SmartModuleSpec>, Error>
where
    AC: AuthContext,
{
    debug!("fetching list of smart modules");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SmartModuleSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            debug!(ty = %SmartModuleSpec::LABEL, "authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    // convert filter into key filter
    //let sm_key_filter = filters.into_iter().map(|filter| filter.into()).collect::<Vec<KeyFilter>>();

    let reader = object_ctx.store().read().await;
    let objects: Vec<Metadata<SmartModuleSpec>> = reader
        .values()
        .filter_map(|value| {
            if filters
                .iter()
                .filter(|filter_value| filter_value.filter(value.key().as_ref()))
                .count()
                > 0
            {
                let list_obj = AdminSpec::convert_from(value);
                Some(list_obj)
            } else {
                None
            }
        })
        .collect();

    debug!(fetch_items = objects.len(),);
    trace!("fetch {:#?}", objects);

    Ok(ListResponse::new(objects))
}
