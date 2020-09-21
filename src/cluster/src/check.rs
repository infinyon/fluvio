use k8_config::{KubeContext};
use url::Url;

use crate::ClusterError;

// Getting server hostname from K8 context
pub(crate) fn get_cluster_server_host(kc_config: KubeContext) -> Result<String, ClusterError> {
    if let Some(ctx) = kc_config.config.current_cluster() {
        let server_url = ctx.cluster.server.to_owned();
        let url = match Url::parse(&server_url) {
            Ok(url) => url,
            Err(e) => {
                return Err(ClusterError::Other(format!(
                    "error parsing server url {}",
                    e.to_string()
                )))
            }
        };
        Ok(url.host().unwrap().to_string())
    } else {
        Err(ClusterError::Other("no context found".to_string()))
    }
}
