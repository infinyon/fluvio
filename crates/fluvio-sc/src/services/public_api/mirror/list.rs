use fluvio_auth::AuthContext;
use fluvio_sc_schema::{
    core::MetadataItem,
    mirror::MirrorSpec,
    objects::{ListFilters, ListResponse, Metadata},
};
use anyhow::Result;
use tracing::{debug, info, trace};

use crate::services::auth::AuthServiceContext;

pub async fn handle_list_mirror<AC: AuthContext, C: MetadataItem>(
    _filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ListResponse<MirrorSpec>> {
    info!("mirror-cluster list");
    let mirror_list: Vec<Metadata<MirrorSpec>> = auth_ctx
        .global_ctx
        .mirrors()
        .store()
        .read()
        .await
        .values()
        .map(|item| item.inner().clone().into())
        .collect();
    debug!("flv fetch mirror list resp: {} items", mirror_list.len());
    trace!("flv fetch mirror list resp {:#?}", mirror_list);

    Ok(ListResponse::new(mirror_list))
}
