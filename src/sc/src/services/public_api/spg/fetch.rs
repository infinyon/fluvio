use std::io::Error;

use tracing::debug;
use tracing::trace;

use fluvio_sc_schema::objects::*;
use fluvio_sc_schema::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::store::*;

use crate::core::Context;

pub async fn handle_fetch_spu_groups_request(
    filters: Vec<NameFilter>,
    ctx: &Context,
) -> Result<ListResponse, Error> {
    debug!("fetching spu groups");
    let spgs: Vec<Metadata<SpuGroupSpec>> = ctx
        .spgs()
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

    debug!("flv fetch spgs resp: {} items", spgs.len());
    trace!("flv fetch spgs resp {:#?}", spgs);

    Ok(ListResponse::SpuGroup(spgs))
}
