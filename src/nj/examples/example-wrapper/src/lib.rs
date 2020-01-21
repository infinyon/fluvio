use std::ptr;


use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::sys::napi_callback_info;
use nj::sys::napi_ref;
use nj::core::register_module;
use nj::core::val::JsEnv;
use nj::core::val::JsExports;
use nj::core::Property;
use nj::core::val::JsCallback;
use nj::core::JSClass;
use nj::core::NjError;
use nj::core::PropertiesBuilder;
use nj::core::result_to_napi;
use nj::core::ToJsValue;
use nj::core::assert_napi;

static mut MYOBJECT_CONSTRUCTOR: napi_ref = ptr::null_mut();

struct MyObject {
    val: f64,
}

impl MyObject {
    pub fn new(val: f64) -> Self {
        Self {
            val
        }
    }

    pub fn plus_one(&mut self) {
        self.val = self.val + 1.0;
    }

    pub fn value(&self) -> f64 {
        self.val
    }

    #[no_mangle]
    pub extern "C" fn js_plus_one(env: napi_env, info: napi_callback_info) -> napi_value {
        println!("invoking plus one method");

        let js_env = JsEnv::new(env);

        let js_cb = result_to_napi!(js_env.get_cb_info(info, 0),&js_env);

        let my_obj = result_to_napi!(js_cb.unwrap::<MyObject>(),&js_env);

        my_obj.plus_one();

        result_to_napi!(js_env.create_double(my_obj.value()),&js_env)
    }

    #[no_mangle]
    pub extern "C" fn js_get_value(env: napi_env, info: napi_callback_info) -> napi_value {
        println!("get value");

        let js_env = JsEnv::new(env);

        let js_cb = result_to_napi!(js_env.get_cb_info(info, 0),&js_env); // there is no argument
        let my_obj = result_to_napi!(js_cb.unwrap::<MyObject>(),&js_env);

        let new_val = my_obj.value();

        println!("rust object value is: {}", new_val);

        result_to_napi!(js_env.create_double(my_obj.value()),&js_env)
    }

    /// generates new object
    #[no_mangle]
    pub extern "C" fn js_multiply(env: napi_env, info: napi_callback_info) -> napi_value {
        let js_env = JsEnv::new(env);

        let js_cb = result_to_napi!(js_env.get_cb_info(info, 1),&js_env); // a single argument
        let my_obj = result_to_napi!(js_cb.unwrap::<MyObject>(),&js_env);

        let arg_value = result_to_napi!(js_cb.get_value::<f64>(0),&js_env);
        let my_val = my_obj.value();

        // multiply two values
        let new_val = result_to_napi!(js_env.create_double(arg_value * my_val),&js_env);

        result_to_napi!(Self::new_instance(&js_env,vec![new_val]),&js_env)
    }
}

impl JSClass for MyObject {
    const CLASS_NAME: &'static str = "MyObject";
    const CONSTRUCTOR_ARG_COUNT: usize = 1;

    fn create_from_js(js_cb: &JsCallback) -> Result<Self, NjError> {
        let value = js_cb.get_value::<f64>(0)?;

        println!("value passed: {}", value);

        Ok(MyObject::new(value))
    }


    fn set_constructor(constructor: napi_ref) {
        unsafe {
            MYOBJECT_CONSTRUCTOR = constructor;
        }
    }

    fn get_constructor() -> napi_ref {
        unsafe { MYOBJECT_CONSTRUCTOR }
    }

    fn properties() -> PropertiesBuilder {
        vec![
            Property::new("plusOne").method(Self::js_plus_one),
            Property::new("multiply").method(Self::js_multiply),
            Property::new("value").getter(Self::js_get_value),
        ]
        .into()
    }
}

/// register all objects
#[no_mangle]
pub extern "C" fn init(env: napi_env, exports: napi_value) -> napi_value {
    let mut js_exports = JsExports::new(env, exports);

    assert_napi!(MyObject::js_init(&mut js_exports));

    return exports;
}

register_module!("hello", init);
