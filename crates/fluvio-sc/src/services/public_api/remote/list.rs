use fluvio_auth::AuthContext;
use fluvio_sc_schema::{
    core::MetadataItem,
    objects::{ListFilters, ListResponse, Metadata},
    remote::RemoteSpec,
};
use anyhow::Result;
use tracing::{debug, info, trace};

use crate::services::auth::AuthServiceContext;

pub async fn handle_list_remote<AC: AuthContext, C: MetadataItem>(
    _filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ListResponse<RemoteSpec>> {
    info!("remote-cluster list");
    let remote_list: Vec<Metadata<RemoteSpec>> = auth_ctx
        .global_ctx
        .remote()
        .store()
        .read()
        .await
        .values()
        .map(|item| item.inner().clone().into())
        .collect();
    debug!("flv fetch remote list resp: {} items", remote_list.len());
    trace!("flv fetch remote list resp {:#?}", remote_list);

    Ok(ListResponse::new(remote_list))
}
