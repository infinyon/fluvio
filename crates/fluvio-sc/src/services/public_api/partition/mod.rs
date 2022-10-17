use std::io::{Error, ErrorKind};

use tracing::{trace, debug, instrument};

use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::partition::{PartitionSpec};
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::services::auth::AuthServiceContext;

#[instrument(skip(_filters, auth_ctx))]
pub async fn handle_fetch_request<AC: AuthContext>(
    _filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<PartitionSpec>, Error> {
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

    Ok(ListResponse::new(partitions))
}
