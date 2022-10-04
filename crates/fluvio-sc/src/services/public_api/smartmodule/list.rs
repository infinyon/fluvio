use std::io::{Error, ErrorKind};

use anyhow::Result;
use tracing::{debug, trace, instrument};

use fluvio_controlplane_metadata::core::Spec;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec, SmartModulePackageKey};
use fluvio_sc_schema::AdminSpec;
use fluvio_stream_dispatcher::store::StoreContext;

use fluvio_sc_schema::objects::{ListResponse, NameFilter, Metadata};
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub(crate) async fn fetch_smart_modules<AC: AuthContext>(
    filters: Vec<NameFilter>,
    auth_ctx: &AuthServiceContext<AC>,
    object_ctx: &StoreContext<SmartModuleSpec>,
) -> Result<ListResponse<SmartModuleSpec>>
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
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error").into());
    }

    // convert filter into key filter
    let mut sm_keys = vec![];
    for filter in filters.into_iter() {
        sm_keys.push(SmartModulePackageKey::from_qualified_name(&filter)?);
    }

    let reader = object_ctx.store().read().await;
    let objects: Vec<Metadata<SmartModuleSpec>> = reader
        .values()
        .filter_map(|value| {
            if sm_keys
                .iter()
                .filter(|filter_value| {
                    filter_value.is_match(
                        value.key().as_ref(),
                        value.spec().meta.as_ref().map(|m| &m.package),
                    )
                })
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
