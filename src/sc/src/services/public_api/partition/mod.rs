use std::io::{Error, ErrorKind};

use tracing::{trace, debug};

use fluvio_sc_schema::objects::{ListResponse,Metadata };
use fluvio_sc_schema::partition::{PartitionSpec};


use crate::services::auth::AuthServiceContext;


pub async fn handle_fetch_request(
    _filters: Vec<String>,
    auth_ctx: &AuthServiceContext,
) -> Result<ListResponse, Error> {
    debug!("fetching custom spu list");

    if let Ok(authorized) = auth_ctx.auth.read::<PartitionSpec>().await {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::Partition(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let partitions: Vec<Metadata<PartitionSpec>> = auth_ctx
        .global_ctx
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
