// wrap ScClient
// JS Wrapper for ScClient

use std::sync::Arc;

use log::debug;

use flv_client::ScClient;
use flv_future_aio::sync::RwLock;
use flv_future_aio::task::run_block_on;
use flv_client::ClientError;

use flv_client::SpuController;
use node_bindgen::derive::node_bindgen;
use node_bindgen::core::TryIntoJs;
use node_bindgen::core::NjError;
use node_bindgen::core::val::JsEnv;
use node_bindgen::sys::napi_value;
use node_bindgen::core::JSClass;

use crate::ReplicaLeaderWrapper;


type SharedScClient = Arc<RwLock<ScClient>>;

// simple wrapper to facilitate conversion to JS Class
pub struct ScClientWrapper(ScClient);

impl From<ScClient> for ScClientWrapper {
    fn from(client: ScClient) -> Self {
        Self(client)
    }
}

impl TryIntoJs for ScClientWrapper {
    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value, NjError> {
        debug!("converting ScClientWrapper to js");
        let new_instance = JsScClient::new_instance(js_env, vec![])?;
        debug!("instance created");
        JsScClient::unwrap_mut(js_env, new_instance)?.set_client(self.0);
        Ok(new_instance)
    }
}

pub struct JsScClient {
    inner: Option<SharedScClient>,
}

#[node_bindgen]
impl JsScClient {
    #[node_bindgen(constructor)]
    pub fn new() -> Self {
        Self { inner: None }
    }

    pub fn set_client(&mut self, client: ScClient) {
        self.inner.replace(Arc::new(RwLock::new(client)));
    }

    fn rust_addr(&self) -> String {
        // since clock is in the lock, we need to read in order to access it
        self.inner.as_ref().map_or("".to_owned(), move |c| {
            run_block_on(async move {
                let c1 = c.clone();
                let read_client = c1.read().await;
                read_client.inner().addr().to_owned()
            })
        })
    }

    /// JS method to return host address
    #[node_bindgen]
    fn addr(&self) -> String {
        self.rust_addr()
    }

    /// find replica
    #[node_bindgen]
    async fn replica(&self, topic: String, partition: i32) -> Result<ReplicaLeaderWrapper, ClientError> {
        let client = self.inner.as_ref().unwrap().clone();
        let mut client_w = client.write().await;
        client_w
            .find_replica_for_topic_partition(&topic, partition)
            .await
            .map(|client| client.into())
    }
}
