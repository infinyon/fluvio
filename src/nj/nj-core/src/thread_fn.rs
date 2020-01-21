use std::ptr;

use crate::sys::napi_threadsafe_function;
use crate::NjError;

/// Wsrapper for Threas safe function that are safe to send and sync across thread
pub struct ThreadSafeFunction(napi_threadsafe_function);

unsafe impl Sync for ThreadSafeFunction{}
unsafe impl Send for ThreadSafeFunction{}

impl From<napi_threadsafe_function> for ThreadSafeFunction {
    fn from(tsf: napi_threadsafe_function) -> Self {
        Self(tsf)
    }
}

impl ThreadSafeFunction {


    pub fn inner(self) -> napi_threadsafe_function {
        self.0
    }

    pub fn call(&self, data: Option<*mut ::std::os::raw::c_void>) -> Result<(),NjError> {
        
        let data_ptr = match data {
            Some(ptr) => ptr,
            None => ptr::null_mut()
        };
        crate::napi_call_result!(
            crate::sys::napi_call_threadsafe_function(
                self.0,
                data_ptr,
                crate::sys::napi_threadsafe_function_call_mode_napi_tsfn_blocking
            )
        )
        
    }
}

impl Drop for ThreadSafeFunction {

    fn drop(&mut self) {
        
        crate::napi_call_assert!(
            crate::sys::napi_release_threadsafe_function(
                self.0,
                crate::sys::napi_threadsafe_function_release_mode_napi_tsfn_release
            )
        );        
    }
}