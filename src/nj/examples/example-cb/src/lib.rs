use std::ptr;

use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::sys::napi_callback_info;
use nj::core::register_module;
use nj::core::val::JsEnv;
use nj::core::val::JsExports;
use nj::core::Property;
use nj::core::result_to_napi;
use nj::core::ToJsValue;

#[no_mangle]
pub extern "C" fn hello_callback(env: napi_env, info: napi_callback_info) -> napi_value {
    let js_env = JsEnv::new(env);
    let cb = result_to_napi!(js_env.get_cb_info(info, 2),&js_env);

    let first_arg = result_to_napi!(cb.get_value::<f64>(0),&js_env);
    let msg = format!("argument is: {}", first_arg);
    let label = result_to_napi!(js_env.create_string_utf8(&msg),&js_env);
    let global = result_to_napi!(js_env.get_global(),&js_env);

    let cb_fn = cb.args(1);
    result_to_napi!(js_env.call_function(global, cb_fn, vec![label]),&js_env);

    ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn init_export(env: napi_env, exports: napi_value) -> napi_value {
    let js_exports = JsExports::new(env, exports);

    js_exports.define_property(
        js_exports
            .prop_builder()
            .add(Property::new("hello").method(hello_callback)),
    ).expect("should be defined");

    exports
}

register_module!("hello", init_export);
