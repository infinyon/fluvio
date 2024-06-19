use std::io::{Error, ErrorKind};

use fluvio_stream_model::core::MetadataItem;
use tracing::{trace, debug, instrument};
use anyhow::Result;

use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::partition::PartitionSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::services::auth::AuthServiceContext;

#[instrument(skip(_filters, auth_ctx))]
pub async fn handle_fetch_request<AC: AuthContext, C: MetadataItem>(
    _filters: ListFilters,
    system: bool,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ListResponse<PartitionSpec>> {
    debug!("fetching custom spu list");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(PartitionSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error").into());
    }

    let partitions: Vec<Metadata<PartitionSpec>> = auth_ctx
        .global_ctx
        .partitions()
        .store()
        .read()
        .await
        .values()
        .filter(|value| value.inner().spec().system == system)
        .map(|value| value.inner().clone().into())
        .collect();

    debug!("flv fetch partitions resp: {} items", partitions.len());
    trace!("flv fetch partitions resp {:#?}", partitions);

    Ok(ListResponse::new(partitions))
}
