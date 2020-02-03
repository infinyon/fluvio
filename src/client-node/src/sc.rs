// wrap ScClient
// JS Wrapper for ScClient

use std::sync::Arc;

use log::debug;

use flv_client::ScClient;
use flv_future_aio::sync::RwLock;
use flv_future_core::run_block_on;
use flv_client::ClientError;

use flv_client::SpuController;
use nj::derive::node_bindgen;
use nj::core::TryIntoJs;
use nj::core::NjError;
use nj::core::val::JsEnv;
use nj::sys::napi_value;
use nj::core::JSClass;

use crate::SpuLeaderWrapper;


type DefaultScClient = ScClient<String>;
type SharedScClient = Arc<RwLock<DefaultScClient>>;

// simple wrapper to facilitate conversion to JS Class
pub struct ScClientWrapper(DefaultScClient);

impl From<DefaultScClient> for ScClientWrapper {
    fn from(client: DefaultScClient) -> Self {
        Self(client)
    }
}


impl TryIntoJs for ScClientWrapper {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {

        debug!("converting ScClientWrapper to js");
        let new_instance = JsScClient::new_instance(js_env,vec![])?;
        debug!("instance created");
        JsScClient::unwrap(js_env,new_instance)?.set_client(self.0);
        Ok(new_instance)

    }
}


pub struct JsScClient {
    inner: Option<SharedScClient>
}

#[node_bindgen]
impl JsScClient {

    #[node_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: None,
        }
    }

    pub fn set_client(&mut self,client: DefaultScClient) {
        self.inner.replace(Arc::new(RwLock::new(client)));
    }

    fn rust_addr(&self) -> String {

        // since clock is in the lock, we need to read in order to access it
        self.inner.as_ref().map_or( "".to_owned(), move |c|  {
            run_block_on( async move {
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


    #[node_bindgen]
    async fn leader(&self,topic: String,partition: i32) -> Result<SpuLeaderWrapper,ClientError>  {
      
        let client = self.inner.as_ref().unwrap().clone();
        let mut client_w = client.write().await;
        client_w.find_leader_for_topic_partition(
                &topic,
                partition).await
                .map( |client| client.into())
        
    }


}


