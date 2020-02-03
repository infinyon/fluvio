// implement connect workflow

use flv_client::profile::ScConfig;
use nj::derive::node_bindgen;
use flv_client::ClientError;

use crate::ScClientWrapper;

#[node_bindgen()]
async fn connect(host_addr: String) -> Result<ScClientWrapper, ClientError> {
    let config = ScConfig::new(Some(host_addr), None)?;
    config.connect().await.map(|client| client.into())
}
