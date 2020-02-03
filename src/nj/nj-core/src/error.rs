
use std::fmt;
use std::ptr;
use std::string::FromUtf8Error;

use crate::sys::napi_status;
use crate::sys::napi_value;
use crate::val::JsEnv;
use crate::IntoJs;


#[derive(Debug)]
pub enum NjError {
    NapiCall(NapiStatus),
    InvalidArgCount(usize,usize),
    InvalidArgIndex(usize,usize),
    InvalidType,
    NoPlainConstructor,
    Utf8Error(FromUtf8Error),
    Other(String)
}

impl IntoJs for NjError {

    fn to_js(self, js_env: &JsEnv) -> napi_value {
        let msg = self.to_string();
        js_env.throw_type_error(&msg);
        ptr::null_mut()
    }
}

impl IntoJs for Result<napi_value,NjError> {
    
    fn to_js(self, js_env: &JsEnv) -> napi_value {
        
        match self {
            Ok(napi_val) => napi_val,
            Err(err) => err.to_js(js_env)
        }
    }
}


impl From<FromUtf8Error> for NjError {
    fn from(error: FromUtf8Error) -> Self {
        Self::Utf8Error(error)
    }
}


impl From<NapiStatus> for NjError {
    fn from(status: NapiStatus) -> Self {
        Self::NapiCall(status)
    }
}


impl fmt::Display for NjError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NapiCall(status) => write!(f,"napi call failed {:#?}",status),
            Self::InvalidType => write!(f,"invalid type"),
            Self::Utf8Error(err) => write!(f,"ut8 error: {}",err),
            Self::InvalidArgIndex(index,len) => write!(f,"attempt to access arg: {} out of len: {}",index,len),
            Self::InvalidArgCount(actual_count,expected_count) => write!(f,"{} args expected but {} is present",expected_count,actual_count),
            Self::NoPlainConstructor => write!(f,"Plain constructor not supported yet"),
            Self::Other(msg) => write!(f,"{}",msg)
        }
    }
}



#[derive(Debug,PartialEq)]
pub enum NapiStatus {
    Ok = crate::sys::napi_status_napi_ok as isize,
    InvalidArg = crate::sys::napi_status_napi_invalid_arg as isize,
    ObjectExpected = crate::sys::napi_status_napi_object_expected as isize,
    StringExpected = crate::sys::napi_status_napi_string_expected as isize,
    NameExpected = crate::sys::napi_status_napi_name_expected as isize,
    FunctionExpected = crate::sys::napi_status_napi_function_expected as isize,
    NumberExpected = crate::sys::napi_status_napi_number_expected as isize,
    BooleanExpected = crate::sys::napi_status_napi_boolean_expected as isize,
    ArrayExpected = crate::sys::napi_status_napi_array_expected as isize,
    GenericFailure = crate::sys::napi_status_napi_generic_failure as isize,
    PendingException = crate::sys::napi_status_napi_pending_exception as isize,
    Cancelled = crate::sys::napi_status_napi_cancelled as isize,
    EscapeCalledTwice = crate::sys::napi_status_napi_escape_called_twice as isize,
    HandleScopeMismatch = crate::sys::napi_status_napi_handle_scope_mismatch as isize,
    CallbackScopeMismatch = crate::sys::napi_status_napi_callback_scope_mismatch as isize,
    QueueFull = crate::sys::napi_status_napi_queue_full as isize,
    Closing = crate::sys::napi_status_napi_closing as isize,
    BigintExpected = crate::sys::napi_status_napi_bigint_expected as isize,
    DateExpected = crate::sys::napi_status_napi_date_expected as isize,
    ArraybufferExpected = crate::sys::napi_status_napi_arraybuffer_expected as isize,
    DetachableArrayBufferExpected = crate::sys::napi_status_napi_detachable_arraybuffer_expected as isize
}

impl From<napi_status> for NapiStatus {

    fn from(status: napi_status) -> Self {
        
        match status {

            crate::sys::napi_status_napi_ok => Self::Ok,
            crate::sys::napi_status_napi_invalid_arg  => Self::InvalidArg,
            crate::sys::napi_status_napi_object_expected => Self::ObjectExpected,
            crate::sys::napi_status_napi_string_expected => Self::StringExpected,
            crate::sys::napi_status_napi_name_expected => Self::NameExpected,
            crate::sys::napi_status_napi_function_expected => Self::FunctionExpected,
            crate::sys::napi_status_napi_number_expected => Self::NumberExpected,
            crate::sys::napi_status_napi_boolean_expected => Self::BooleanExpected,
            crate::sys::napi_status_napi_array_expected => Self::ArrayExpected,
            crate::sys::napi_status_napi_generic_failure => Self::GenericFailure,
            crate::sys::napi_status_napi_pending_exception => Self::PendingException,
            crate::sys::napi_status_napi_cancelled => Self::Cancelled,
            crate::sys::napi_status_napi_escape_called_twice => Self::EscapeCalledTwice,
            crate::sys::napi_status_napi_handle_scope_mismatch => Self::HandleScopeMismatch,
            crate::sys::napi_status_napi_callback_scope_mismatch => Self::CallbackScopeMismatch,
            crate::sys::napi_status_napi_queue_full => Self::QueueFull,
            crate::sys::napi_status_napi_closing => Self::Closing,
            crate::sys::napi_status_napi_bigint_expected => Self::BigintExpected,
            crate::sys::napi_status_napi_date_expected => Self::DateExpected,
            crate::sys::napi_status_napi_arraybuffer_expected => Self::ArraybufferExpected,
            crate::sys::napi_status_napi_detachable_arraybuffer_expected  => Self::DetachableArrayBufferExpected,
            _ => panic!("cannot convert: {}",status)

        }
    }

} 

