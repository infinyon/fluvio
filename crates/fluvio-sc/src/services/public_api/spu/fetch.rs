use std::io::{Error, ErrorKind};

use tracing::{trace, debug, instrument};

use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::spu::SpuSpec;
use fluvio_sc_schema::customspu::CustomSpuSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_custom_spu_request<AC: AuthContext>(
    filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<CustomSpuSpec>, Error> {
    debug!("fetching custom spu list");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(CustomSpuSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let custom_spus: Vec<_> = auth_ctx
        .global_ctx
        .spus()
        .store()
        .read()
        .await
        .values()
        .filter_map(|value| {
            if value.spec().is_custom() && filters.filter(value.key()) {
                Some(value.inner().clone().into())
            } else {
                None
            }
        })
        .map(|spu: Metadata<SpuSpec>| Metadata {
            name: spu.name,
            spec: spu.spec.into(),
            status: spu.status,
        })
        .collect();

    debug!("flv fetch custom resp: {} items", custom_spus.len());
    trace!("flv fetch custom spus resp {:#?}", custom_spus);

    Ok(ListResponse::new(custom_spus))
}

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_spus_request<AC: AuthContext>(
    filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<SpuSpec>, Error> {
    debug!("fetching spu list");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SpuSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::new(vec![]));
        }
    }

    let spus: Vec<Metadata<SpuSpec>> = auth_ctx
        .global_ctx
        .spus()
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

    debug!("fetched {} spu items", spus.len());
    trace!("fetch spus items detail: {:#?}", spus);

    Ok(ListResponse::new(spus))
}
