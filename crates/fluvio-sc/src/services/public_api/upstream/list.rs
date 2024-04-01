use fluvio_auth::AuthContext;
use fluvio_controlplane_metadata::upstream_cluster::UpstreamClusterSpec;
use fluvio_sc_schema::{
    core::MetadataItem,
    objects::{ListFilters, ListResponse, Metadata},
};
use anyhow::Result;
use tracing::{debug, info, trace};

use crate::services::auth::AuthServiceContext;

pub async fn handle_list_upstream<AC: AuthContext, C: MetadataItem>(
    _filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ListResponse<UpstreamClusterSpec>> {
    info!("upstream-cluster list");
    let upstream_list: Vec<Metadata<UpstreamClusterSpec>> = auth_ctx
        .global_ctx
        .upstream_clusters()
        .store()
        .read()
        .await
        .values()
        .map(|item| {
            let name = item.key.clone();
            Metadata {
                name,
                spec: item.spec.clone(),
                status: item.status.clone(),
            }
        })
        .collect();
    debug!("flv fetch upstream list resp: {} items", upstream_list.len());
    trace!("flv fetch upstream list resp {:#?}", upstream_list);

    Ok(ListResponse::new(upstream_list))
}
