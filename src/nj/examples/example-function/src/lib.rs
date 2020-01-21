use std::ptr;

use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::sys::napi_callback_info;
use nj::core::register_module;
use nj::core::c_str;
use nj::core::val::JsEnv;
use nj::core::val::JsExports;
use nj::core::Property;
use nj::core::result_to_napi;
use nj::core::ToJsValue;


#[no_mangle]
pub extern "C" fn hello_world(env: napi_env, cb_info: napi_callback_info) -> napi_value {

    fn rust_hello_world(count: i32) -> String {        
        format!("hello world {}", count)
    }
    
    let js_env = JsEnv::new(env);
    let js_cb = result_to_napi!(js_env.get_cb_info(cb_info, 1),&js_env);
    let msg = result_to_napi!(js_cb.get_value::<i32>(0),&js_env);
    rust_hello_world(msg).to_js(&js_env)
}


#[no_mangle]
pub extern "C" fn init_hello(env: napi_env, exports: napi_value) -> napi_value {
    let js_exports = JsExports::new(env, exports);
    let prop = js_exports
        .prop_builder()
        .add(Property::new("hello").method(hello_world));

    js_exports.define_property(prop).expect("property should not fail");

    return exports;
}

register_module!("hello", init_hello);
