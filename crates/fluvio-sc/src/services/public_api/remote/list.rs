use fluvio_auth::AuthContext;
use fluvio_controlplane_metadata::remote_cluster::RemoteClusterSpec;
use fluvio_sc_schema::{
    core::MetadataItem,
    objects::{ListFilters, ListResponse, Metadata},
};
use anyhow::Result;
use tracing::{debug, info, trace};

use crate::services::auth::AuthServiceContext;

pub async fn handle_list_remote<AC: AuthContext, C: MetadataItem>(
    _filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ListResponse<RemoteClusterSpec>> {
    info!("remote-cluster list");
    let remote_list: Vec<Metadata<RemoteClusterSpec>> = auth_ctx
        .global_ctx
        .remote_clusters()
        .store()
        .read()
        .await
        .values()
        .map(|item| {
            let name = item.key.clone();
            // let remote_type = item.spec.remote_type.to_string();
            // ListItem {
            //     name,
            //     remote_type,
            //     pairing: "-".into(),
            //     status: "-".into(),
            //     last: "-".into(),
            // }
            Metadata {
                name,
                spec: item.spec.clone(),
                status: item.status.clone(),
            }
        })
        .collect();
    debug!("flv fetch remote list resp: {} items", remote_list.len());
    trace!("flv fetch remote list resp {:#?}", remote_list);

    Ok(ListResponse::new(remote_list))
}
