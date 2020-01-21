mod basic;
mod error;
mod thread_fn;
mod property;
mod class;
mod worker;
mod convert;

pub use thread_fn::ThreadSafeFunction;
pub use error::NjError;
pub use error::NapiStatus;
pub use property::Property;
pub use property::PropertiesBuilder;
pub use class::JSClass;
pub use worker::create_promise;
pub use convert::ToJsValue;
pub use ctor::ctor;

use class::JSObjectWrapper;

pub mod sys {
    pub use nj_sys::*;
}

pub mod val {
    pub use crate::basic::*;
}

pub mod log {
    pub use ::log::*;
}


/// call napi and assert
/// used only in this crate
#[macro_export]
macro_rules! napi_call_assert {
    ($napi_expr:expr) =>  {
        {
            let status = unsafe { $napi_expr };
            if status !=  crate::sys::napi_status_napi_ok {
                let nj_status: crate::NapiStatus = status.into();
                log::error!("error executing napi call {:#?}",nj_status);
            }
        }
    }
}


/// call napi and wrap into result
/// used only in this crate
#[macro_export]
macro_rules! napi_call_result {
    ($napi_expr:expr) =>  {
        {
            let status = unsafe { $napi_expr };
            if  status == crate::sys::napi_status_napi_ok  {
                Ok(())
            } else { 
                let nj_status: crate::NapiStatus = status.into();
                log::error!("error executing napi call {:#?}",nj_status);
                Err(crate::NjError::NapiCall(nj_status))
            }
        }
    }
}

/// convert result into napi value if ok otherwise convert to error
#[macro_export]
macro_rules! result_to_napi {
    ($result:expr,$js_env:expr) =>  {
        
        match $result {
            Ok(val) => val,
            Err(err) => {
                return err.to_js($js_env);
            }
        }
        
    }
}

#[macro_export]
macro_rules! callback_to_napi {
    ($result:expr,$js_env:expr) =>  {
        
        match $result {
            Ok(val) => val,
            Err(err) => {
                return;
            }
        }
        
    }
}

/// assert the napi call
#[macro_export]
macro_rules! assert_napi {
    ($result:expr) =>  {
        
        match $result {
            Ok(val) => val,
            Err(err) => {
                panic!("napi call failed: {}",err)
            }
        }
        
    }
}



#[macro_export]
macro_rules! c_str {
    ($string:literal) =>  {
        {
            const _C_STRING: &'static str = concat!($string,"\0");       
            _C_STRING
        }
    }
}

pub fn init_logger() {
    utils::init_logger();
}

mod init_module {


    #[macro_export]
    macro_rules! register_module {
    
        ($name:literal,$reg_fn:ident) => { 
                
            #[nj::core::ctor]
            fn init_module() {

                use nj::core::c_str;
                use nj::core::sys::NAPI_VERSION;
                use nj::core::sys::napi_module;

                extern "C" {
                    pub fn napi_module_register(mod_: *mut napi_module);
                }

                static mut _module: napi_module  = napi_module {
                    nm_version: NAPI_VERSION as i32,
                    nm_flags: 0,
                    nm_filename: c_str!("lib.rs").as_ptr() as *const i8,
                    nm_register_func: Some($reg_fn),
                    nm_modname:  c_str!($name).as_ptr() as *const i8,
                    nm_priv: ptr::null_mut(),
                    reserved: [ptr::null_mut(),ptr::null_mut(),ptr::null_mut(),ptr::null_mut()]
                };

                unsafe {
                    napi_module_register(&mut _module);
                }

                nj::core::init_logger();

            }
            
        }
    }

    
}


