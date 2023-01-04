use std::io::{Error, ErrorKind};
use std::fmt::Debug;

use anyhow::Result;
use tracing::{debug, trace, instrument};

use fluvio_controlplane_metadata::core::{Spec, MetadataItem};
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec, SmartModulePackageKey};
use fluvio_sc_schema::AdminSpec;
use fluvio_stream_dispatcher::store::StoreContext;

use fluvio_sc_schema::objects::{ListResponse, Metadata};
use fluvio_sc_schema::objects::ListFilter;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;

#[instrument(skip(filters, auth, object_ctx))]
pub(crate) async fn fetch_smart_modules<AC: AuthContext, M>(
    filters: Vec<ListFilter>,
    summary: bool,
    auth: &AC,
    object_ctx: &StoreContext<SmartModuleSpec, M>,
) -> Result<ListResponse<SmartModuleSpec>>
where
    AC: AuthContext,
    M: MetadataItem + Debug,
{
    debug!("fetching list of smartmodules");

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
        sm_keys.push(SmartModulePackageKey::from_qualified_name(&filter.name)?);
    }

    let reader = object_ctx.store().read().await;
    let objects: Vec<Metadata<SmartModuleSpec>> = reader
        .values()
        .filter_map(|value| {
            //println!("value: {:#?}", value);
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
                debug!("found matching smartmodule: {:#?}", value.spec);
                if summary {
                    Some(Metadata {
                        name: value.key().clone(),
                        spec: value.spec().clone().summary(),
                        status: value.status().clone(),
                    })
                } else {
                    Some(AdminSpec::convert_from(value))
                }
            } else {
                None
            }
        })
        .collect();

    debug!(fetched_items = objects.len(),);
    trace!("fetch {:#?}", objects);

    Ok(ListResponse::new(objects))
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use fluvio_stream_dispatcher::store::StoreContext;
    use fluvio_stream_model::fixture::TestMeta;
    use fluvio_stream_model::store::{MetadataStoreObject, LocalStore};
    use fluvio_controlplane_metadata::smartmodule::{
        SmartModuleSpec, SmartModuleMetadata, SmartModulePackage, FluvioSemVersion,
    };

    use crate::{
        services::auth::{RootAuthContext},
    };

    use super::fetch_smart_modules;

    type TestSmartModuleStore = LocalStore<SmartModuleSpec, TestMeta>;
    type SmartModuleTest = MetadataStoreObject<SmartModuleSpec, TestMeta>;

    #[fluvio_future::test]
    async fn test_sm_search() {
        let root_auth = RootAuthContext {};

        let pkg = SmartModulePackage {
            name: "sm2".to_string(),
            group: "group".to_string(),
            version: FluvioSemVersion::parse("0.1.0").unwrap(),
            api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
            ..Default::default()
        };

        assert_ne!(pkg.store_id(), "sm2");

        let test_data = vec![
            SmartModuleTest::with_spec("sm1", SmartModuleSpec::default()),
            SmartModuleTest::with_spec(
                pkg.store_id(),
                SmartModuleSpec {
                    meta: Some(SmartModuleMetadata {
                        package: pkg,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ];

        let local_sm_store = TestSmartModuleStore::default();
        _ = local_sm_store.sync_all(test_data).await;
        let sm_ctx = StoreContext::new_with_store(Arc::new(local_sm_store));
        assert_eq!(sm_ctx.store().read().await.len(), 2);
        assert_eq!(
            fetch_smart_modules(vec![], false, &root_auth, &sm_ctx)
                .await
                .expect("search")
                .inner()
                .len(),
            2
        );
        assert_eq!(
            fetch_smart_modules(vec!["test".to_owned().into()], false, &root_auth, &sm_ctx)
                .await
                .expect("search")
                .inner()
                .len(),
            0
        );

        assert_eq!(
            fetch_smart_modules(vec!["sm1".to_owned().into()], false, &root_auth, &sm_ctx)
                .await
                .expect("search")
                .inner()
                .len(),
            1
        );

        // no matching
        assert_eq!(
            fetch_smart_modules(vec!["sm2".to_owned().into()], false, &root_auth, &sm_ctx)
                .await
                .expect("search")
                .inner()
                .len(),
            1
        );
    }
}
