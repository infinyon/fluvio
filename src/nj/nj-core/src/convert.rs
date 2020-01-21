use crate::sys::napi_value;
use crate::val::JsEnv;
use crate::result_to_napi;

/// convert to JS object
pub trait ToJsValue {

    fn to_js(self,js_env: &JsEnv) -> napi_value;    

}


impl ToJsValue for f64 {

    fn to_js(self, js_env: &JsEnv) -> napi_value {
        result_to_napi!(js_env.create_double(self),js_env)
    }
}

impl ToJsValue for i64 {

    fn to_js(self, js_env: &JsEnv) -> napi_value {
        result_to_napi!(js_env.create_int64(self),js_env)
    }
}

impl ToJsValue for i32 {

    fn to_js(self, js_env: &JsEnv) -> napi_value {
        result_to_napi!(js_env.create_int32(self),js_env)
    }
}


impl ToJsValue for String {

    fn to_js(self, js_env: &JsEnv) -> napi_value {
        result_to_napi!(js_env.create_string_utf8(&self),js_env)
    }

}
