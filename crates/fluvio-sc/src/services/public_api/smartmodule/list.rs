use std::io::{Error, ErrorKind};
use std::fmt::Debug;

use anyhow::Result;
use tracing::{debug, trace, instrument};

use fluvio_controlplane_metadata::core::{Spec, MetadataItem};
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec, SmartModulePackageKey};
use fluvio_sc_schema::AdminSpec;
use fluvio_stream_dispatcher::store::StoreContext;

use fluvio_sc_schema::objects::{ListResponse, NameFilter, Metadata};
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;

#[instrument(skip(filters, auth))]
pub(crate) async fn fetch_smart_modules<AC: AuthContext, M>(
    filters: Vec<NameFilter>,
    auth: &AC,
    object_ctx: &StoreContext<SmartModuleSpec, M>,
) -> Result<ListResponse<SmartModuleSpec>>
where
    AC: AuthContext,
    M: MetadataItem + Debug,
{
    debug!("fetching list of smart modules");

    if let Ok(authorized) = auth
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
            if sm_keys.is_empty()
                || sm_keys
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

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use fluvio_stream_dispatcher::store::StoreContext;
    use fluvio_stream_model::fixture::TestMeta;
    use fluvio_stream_model::store::{MetadataStoreObject, LocalStore};
    use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;

    use crate::{
        services::auth::{RootAuthContext},
    };

    use super::fetch_smart_modules;

    type TestSmartModuleStore = LocalStore<SmartModuleSpec, TestMeta>;
    type SmartModuleTest = MetadataStoreObject<SmartModuleSpec, TestMeta>;

    #[fluvio_future::test]
    async fn test_sm_search() {
        let root_auth = RootAuthContext {};

        let sm1 = vec![SmartModuleTest::with_spec(
            "sm1",
            SmartModuleSpec::default(),
        )];
        let local_sm_store = TestSmartModuleStore::default();
        _ = local_sm_store.sync_all(sm1).await;
        let sm_ctx = StoreContext::new_with_store(Arc::new(local_sm_store));
        let search = fetch_smart_modules(vec![], &root_auth, &sm_ctx).await;
    }
}
