// implement connect workflow
use nj::sys::napi_callback_info;
use nj::sys::napi_env;
use nj::sys::napi_value;
use nj::core::val::JsEnv;
use nj::core::create_promise;
use nj::core::result_to_napi;
use nj::core::ToJsValue;
use nj::core::NjError;
use flv_client::profile::ScConfig;

use crate::ScClientWrapper;
use crate::JsClientError;


pub extern "C" fn sc_connect(env: napi_env, info: napi_callback_info) -> napi_value {

    fn inner_connect(js_env: &JsEnv, inner_info: napi_callback_info) -> Result<napi_value,NjError> {

        let js_cb = js_env.get_cb_info(inner_info,1)?;  
        let host_addr = js_cb.get_value::<String>(0)?;

        create_promise(&js_env,"sc_connect_ft",async move{
            let config = ScConfig::new(Some(host_addr),None)?;
            config.connect().await
                .map( |client| client.into())
            .map_err( |err| err.into()) as Result<ScClientWrapper,JsClientError>
        })
    }

    let js_env = JsEnv::new(env);
    result_to_napi!(inner_connect(&js_env,info),&js_env)
}
