use std::ptr;
use std::time::Duration;

use flv_future_core::sleep;
use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::sys::napi_callback_info;
use nj::core::val::JsEnv;
use nj::core::create_promise;
use nj::core::val::JsExports;
use nj::core::Property;
use nj::core::NjError;
use nj::core::register_module;
use nj::core::result_to_napi;
use nj::core::ToJsValue;


// function that returns promise

extern "C" fn hello_function(env: napi_env, info: napi_callback_info) -> napi_value {

    let js_env = JsEnv::new(env);
    let js_cb = result_to_napi!(js_env.get_cb_info(info,1),&js_env); 
    let my_data = result_to_napi!(js_cb.get_value::<f64>(0),&js_env);

    result_to_napi!(create_promise(&js_env,"hello_ft",async move{
        println!("sleeping");
        sleep(Duration::from_secs(1)).await;
        println!("woke and adding 10.0");
        Ok(my_data + 10.0) as Result<f64,NjError>
    }),&js_env)
}



#[no_mangle]
pub extern "C" fn init_export (env: napi_env, exports: napi_value ) -> napi_value {
    

    let js_exports = JsExports::new(env,exports);
    
    js_exports.define_property(
        js_exports.prop_builder()
            .add(
                Property::new("hello")
                .method(hello_function))).expect("property should be defined");
    
    exports

}
  

register_module!("hello",init_export);
