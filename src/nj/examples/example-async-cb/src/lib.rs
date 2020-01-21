use std::ptr;
use std::time::Duration;

use flv_future_core::spawn;
use flv_future_core::sleep;
use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::sys::napi_callback_info;
use nj::core::val::JsExports;
use nj::core::Property;
use nj::core::register_module;
use nj::core::val::JsEnv;
use nj::core::result_to_napi;
use nj::core::callback_to_napi;
use nj::core::ToJsValue;

// convert the rust data into JS
pub extern "C" fn hello_callback_js(
    env: napi_env,
    js_cb: napi_value, 
    _context: *mut ::std::os::raw::c_void,
    data: *mut ::std::os::raw::c_void) {

    if env != ptr::null_mut() {

        let js_env = JsEnv::new(env);
        let global = callback_to_napi!(js_env.get_global(),&js_env);

        let my_val: Box<f64> = unsafe { Box::from_raw(data as *mut f64) };

        let label = callback_to_napi!(js_env.create_string_utf8("hello world"),&js_env);
        let value = callback_to_napi!(js_env.create_double(*my_val),&js_env);
        callback_to_napi!(js_env.call_function(global,js_cb,vec![value,label]),&js_env);
    }
    
}


#[no_mangle]
pub extern "C" fn hello_callback_async(env: napi_env,info: napi_callback_info) -> napi_value {
  
    
    let js_env = JsEnv::new(env); 
    let js_cb = result_to_napi!(js_env.get_cb_info(info,1),&js_env);    // only has 1 argument

    let xtsfn = result_to_napi!(js_cb.create_thread_safe_function("async",0,Some(hello_callback_js)),&js_env);


    spawn(async move {
            
            println!("sleeping");
            sleep(Duration::from_secs(1)).await;
            println!("woke from time");

            // create new object 
            let my_val: f64 = 10.0;
            let my_box = Box::new(my_val);
            let ptr = Box::into_raw(my_box);

            xtsfn.call(Some(ptr as *mut core::ffi::c_void)).expect("callback should work");
    });

    return ptr::null_mut()

  }



#[no_mangle]
pub extern "C" fn init_export (env: napi_env, exports: napi_value ) -> napi_value {
    
    let js_exports = JsExports::new(env,exports);
    
    js_exports.define_property(
        js_exports.prop_builder()
        .add(
            Property::new("hello")
            .method(hello_callback_async))).expect("should be defined");
    
    exports
}
  

register_module!("hello",init_export);
