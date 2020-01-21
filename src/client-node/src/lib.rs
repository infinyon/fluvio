
mod connect;
mod sc;
mod spu_leader;
mod consume_stream;

use crate::sc::ScClientWrapper;
use crate::spu_leader::SpuLeaderWrapper;
use convert::JsClientError;

mod init {

    use std::ptr;

    use nj::sys::napi_value;
    use nj::sys::napi_env;
    use nj::core::register_module;
    use nj::core::val::JsExports;
    use nj::core::Property;
    use nj::core::JSClass;

    use crate::connect::sc_connect;
    use crate::sc::JsScClient;
    use crate::spu_leader::JsSpuLeader;

    #[no_mangle]
    pub extern "C" fn init_export (env: napi_env, exports: napi_value ) -> napi_value {

        let mut js_exports = JsExports::new(env,exports);
        let prop = js_exports.prop_builder().add(
                Property::new("connectSc")
                    .method(sc_connect));
        
        js_exports.define_property(prop).expect("property should be defined");

        JsScClient::js_init(&mut js_exports).expect("init should not fail");
        JsSpuLeader::js_init(&mut js_exports).expect("init should not fail");
    
        exports
    }
  
    register_module!("flv-node",init_export);
}


mod convert {

    use std::ptr;

    use flv_client::ClientError;
    use nj::sys::napi_value;
    use nj::core::ToJsValue;
    use nj::core::val::JsEnv;

    pub struct JsClientError(ClientError);

    impl From<ClientError> for JsClientError {
        fn from(error: ClientError) -> Self {
            Self(error)
        }
    }

    impl ToJsValue for JsClientError {
        fn to_js(self, _js_env: &JsEnv) -> napi_value {
            ptr::null_mut()
        }
    }
}