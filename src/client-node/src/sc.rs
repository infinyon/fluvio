// wrap ScClient
// JS Wrapper for ScClient

use std::ptr;
use std::sync::Arc;

use log::debug;
use flv_client::ScClient;
use flv_future_aio::sync::RwLock;
use flv_future_core::run_block_on;
use nj::core::JSClass;
use nj::core::NjError;
use nj::core::val::JsEnv;
use nj::sys::napi_ref;
use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::sys::napi_callback_info;
use nj::core::Property;
use nj::core::val::JsCallback;
use nj::core::PropertiesBuilder;
use nj::core::ToJsValue;
use nj::core::create_promise;
use nj::core::result_to_napi;
use flv_client::SpuController;

use crate::SpuLeaderWrapper;
use crate::JsClientError;


static mut JS_CLIENT_CONSTRUCTOR: napi_ref = ptr::null_mut();

type DefaultScClient = ScClient<String>;
type SharedScClient = Arc<RwLock<DefaultScClient>>;

// simple wrapper to facilitate conversion to JS Class
pub struct ScClientWrapper(DefaultScClient);

impl From<DefaultScClient> for ScClientWrapper {
    fn from(client: DefaultScClient) -> Self {
        Self(client)
    }
}


impl ToJsValue for ScClientWrapper {

    fn to_js(self, js_env: &JsEnv) -> napi_value {

        debug!("converting ScClientWrapper to js");
        let instance_result = JsScClient::new_instance(js_env,vec![]);
        debug!("instance created");
        let new_instance = result_to_napi!(instance_result,js_env);
        result_to_napi!(JsScClient::unwrap(js_env,new_instance),js_env).set_client(self.0);
        new_instance

    }
}





pub struct JsScClient {
    inner: Option<SharedScClient>
}


impl JsScClient {

    pub fn new() -> Self {
        Self {
            inner: None,
        }
    }

    pub fn set_client(&mut self,client: DefaultScClient) {
        self.inner.replace(Arc::new(RwLock::new(client)));
    }

    fn addr(&self) -> String {

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
    #[no_mangle]
    pub extern "C" fn js_addr(env: napi_env, info: napi_callback_info) -> napi_value {
      
        let js_env = JsEnv::new(env);

        let js_cb = result_to_napi!(js_env.get_cb_info(info, 0),&js_env); // there is no argument

        let js_client = result_to_napi!(js_cb.unwrap::<Self>(),&js_env);

        let addr = js_client.addr();

        result_to_napi!(js_env.create_string_utf8(&addr),&js_env)
    }


    #[no_mangle]
    pub extern "C" fn js_find_leader_for_topic_partition(env: napi_env, info: napi_callback_info) -> napi_value {
      
        let js_env = JsEnv::new(env);

        let js_cb = result_to_napi!(js_env.get_cb_info(info, 2),&js_env); // there is 2 argument

        let topic = result_to_napi!(js_cb.get_value::<String>(0),&js_env);
        let partition = result_to_napi!(js_cb.get_value::<i32>(1),&js_env);
        let js_client = result_to_napi!(js_cb.unwrap::<Self>(),&js_env);

        if let Some(ref ref_client) = js_client.inner {
           
            let client = ref_client.clone();
            result_to_napi!(create_promise(&js_env,"find_leader_ft", async move {
                let mut client_w = client.write().await;
                client_w.find_leader_for_topic_partition(
                    &topic,
                    partition).await
                    .map( |client| client.into())
                    .map_err( |err| err.into()) as Result<SpuLeaderWrapper,JsClientError>
                }),&js_env)

        } else {
            println!("client was not initialized properly");
            ptr::null_mut()
        }

        
    }


}



impl JSClass for JsScClient {

    const CLASS_NAME: &'static str = "ScClient";

    const CONSTRUCTOR_ARG_COUNT: usize = 0;

    fn create_from_js(_js_cb: &JsCallback) -> Result<Self, NjError> {

        debug!("creating ScClient");
        Ok(Self::new())
    }

    fn set_constructor(constructor: napi_ref) {
        unsafe {
            JS_CLIENT_CONSTRUCTOR = constructor;
        }
    }

    fn get_constructor() -> napi_ref {
        unsafe { JS_CLIENT_CONSTRUCTOR }
    }


    fn properties() -> PropertiesBuilder {
        vec![
            Property::new("addr").method(Self::js_addr),
            Property::new("findLeader").method(Self::js_find_leader_for_topic_partition)
        ]
        .into()
    }

}


