use std::io::Error;

use tracing::{trace, debug};

use fluvio_controlplane_api::objects::*;
use fluvio_controlplane_api::spu::SpuSpec;
use fluvio_controlplane_api::spu::CustomSpuSpec;
use fluvio_metadata::store::*;
use crate::core::Context;

pub async fn handle_fetch_custom_spu_request(
    filters: Vec<String>,
    ctx: &Context,
) -> Result<ListResponse, Error> {
    debug!("fetching custom spu list");
    let spus: Vec<Metadata<SpuSpec>> = ctx
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
        .collect();

    let custom_spus: Vec<Metadata<CustomSpuSpec>> = spus
        .into_iter()
        .map(|spu| Metadata {
            name: spu.name,
            spec: spu.spec.into(),
            status: spu.status,
        })
        .collect();

    debug!("flv fetch custom resp: {} items", custom_spus.len());
    trace!("flv fetch custom spus resp {:#?}", custom_spus);

    Ok(ListResponse::CustomSpu(custom_spus))
}

pub async fn handle_fetch_spus_request(
    filters: Vec<String>,
    ctx: &Context,
) -> Result<ListResponse, Error> {
    debug!("fetching spu list");

    let spus: Vec<Metadata<SpuSpec>> = ctx
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

    Ok(ListResponse::Spu(spus))
}
