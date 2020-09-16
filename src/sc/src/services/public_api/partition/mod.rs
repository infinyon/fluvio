use std::io::Error;

use tracing::{trace, debug};

use fluvio_sc_schema::objects::*;
use fluvio_sc_schema::partition::*;
use crate::core::Context;

pub async fn handle_fetch_request(
    _filters: Vec<String>,
    ctx: &Context,
) -> Result<ListResponse, Error> {
    debug!("fetching custom spu list");
    let partitions: Vec<Metadata<PartitionSpec>> = ctx
        .partitions()
        .store()
        .read()
        .await
        .values()
        .map(|value| value.inner().clone().into())
        .collect();

    debug!("flv fetch partitions resp: {} items", partitions.len());
    trace!("flv fetch partitions resp {:#?}", partitions);

    Ok(ListResponse::Partition(partitions))
}
