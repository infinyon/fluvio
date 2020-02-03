use crate::sys::napi_value;
use crate::val::JsEnv;
use crate::NjError;


/// convert to JS object
pub trait TryIntoJs {

    fn try_to_js(self,js_env: &JsEnv) -> Result<napi_value,NjError> ;    
    
}


impl TryIntoJs for f64 {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        js_env.create_double(self)
    }
}

impl TryIntoJs for i64 {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        js_env.create_int64(self)
    }
}

impl TryIntoJs for i32 {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        js_env.create_int32(self)
    }
}


impl TryIntoJs for String {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        js_env.create_string_utf8(&self)
    }

}

impl TryIntoJs for () {
    fn try_to_js(self, _js_env: &JsEnv) -> Result<napi_value,NjError> {
        Ok(std::ptr::null_mut())
    }

}


impl <T,E>TryIntoJs for Result<T,E> where T: TryIntoJs, E: ToString {
    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        match self {
            Ok(val) => val.try_to_js(&js_env),
            Err(err) =>  Err(NjError::Other(err.to_string()))
        }
    }
}

impl TryIntoJs for napi_value {
    fn try_to_js(self,_js_env: &JsEnv) -> Result<napi_value,NjError> {
        Ok(self)
    }
}

/// convert to js including error
pub trait IntoJs {

    fn to_js(self,js_env: &JsEnv) -> napi_value;

}