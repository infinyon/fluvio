use std::ptr;
use std::ffi::CString;

use libc::size_t;
use log::error;
use log::debug;
use log::trace;

use crate::sys::napi_env;
use crate::sys::napi_value;
use crate::sys::napi_callback_info;
use crate::sys::napi_callback_raw;
use crate::sys::napi_finalize_raw;
use crate::sys::napi_valuetype;
use crate::sys::napi_ref;
use crate::sys::napi_deferred;
use crate::sys::napi_threadsafe_function_call_js;

use crate::napi_call_result;
use crate::napi_call_assert;
use crate::PropertiesBuilder;
use crate::NjError;
use crate::JSObjectWrapper;


#[derive(Clone)]
pub struct JsNapiValue(napi_value);

unsafe impl Send for JsNapiValue{}

impl From<napi_value> for JsNapiValue {
    fn from(value: napi_value) -> Self {
        Self(value)
    }
}

#[derive(Clone,Debug)]
pub struct JsEnv(napi_env);

impl From<napi_env> for JsEnv {
    fn from(env: napi_env) -> Self {
        Self(env)
    }
}

impl JsEnv {

    pub fn new(env: napi_env) -> Self {
        Self(env)
    }

    pub fn inner(&self) -> napi_env {
        self.0
    }


    pub fn create_string_utf8(&self,r_string: &str)  -> Result<napi_value,NjError> {

        use nj_sys::napi_create_string_utf8;
        
        let mut js_value = ptr::null_mut();
        napi_call_result!(
            napi_create_string_utf8(
                self.0,
                r_string.as_ptr() as *const i8,
                r_string.len(),
                &mut js_value
            ) 
        )?;
        Ok(js_value)
    }

    pub fn create_string_utf8_from_bytes(&self,r_string: &Vec<u8>)  -> Result<napi_value,NjError> {

        use nj_sys::napi_create_string_utf8;

        let mut js_value = ptr::null_mut();
        napi_call_result!(
            napi_create_string_utf8(
                self.0,
                r_string.as_ptr() as *const i8,
                r_string.len(),
                &mut js_value
            ) 
        )?;
        Ok(js_value)
    }

    pub fn create_double(&self,value: f64) -> Result<napi_value,NjError> {

        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_create_double(
                self.0,
                value,
                &mut result
            )
        )?;
        Ok(result)
    }

    pub fn create_int64(&self,value: i64) -> Result<napi_value,NjError> {

        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_create_int64(
                self.0,
                value,
                &mut result
            )
        )?;
        Ok(result)
    }

    pub fn create_int32(&self,value: i32) -> Result<napi_value,NjError> {

        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_create_int32(
                self.0,
                value,
                &mut result
            )
        )?;
        Ok(result)
    }



    pub fn get_global(&self) -> Result<napi_value,NjError> {

        use nj_sys::napi_get_global;

        let mut js_global = ptr::null_mut();
        napi_call_result!(
            napi_get_global(self.0, &mut js_global)
        )?;
        Ok(js_global)
    }

    pub fn call_function(&self,
        recv: napi_value,
        func: napi_value,
        mut argv: Vec<napi_value>,
        ) -> Result<napi_value,NjError> {

        use nj_sys::napi_call_function;
            
        let mut result = ptr::null_mut();

        napi_call_result!(
            napi_call_function(
                self.0,
                recv,
                func,
                argv.len(),
                argv.as_mut_ptr(),
                &mut result
            )
        )?;

        Ok(result)
    }

    /// get callback with argument size
    pub fn get_cb_info(&self,info: napi_callback_info,arg_count: usize) -> Result<JsCallback,NjError> {

        use nj_sys::napi_get_cb_info;

        let mut this = ptr::null_mut();

        let args = if arg_count == 0 {
            napi_call_result!(
                napi_get_cb_info(
                    self.0, 
                    info,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    &mut this,
                    ptr::null_mut()
                ))?;
            vec![]

        } else {
            let mut argc: size_t  = arg_count as size_t;
            let mut args = vec![ptr::null_mut();arg_count];
            napi_call_result!(
                napi_get_cb_info(
                    self.0, 
                    info,
                    &mut argc,
                    args.as_mut_ptr(),
                    &mut this,
                    ptr::null_mut()
                ))?;

            if argc != arg_count {
                debug!("expected {}, actual {}",arg_count,argc);
                return Err(NjError::InvalidArgCount(argc as usize,arg_count));
            }
            args
        };
    

        Ok(JsCallback {
            env: JsEnv::new(self.0),
            args,
            this
            
        })
    }

    /// define classes
    pub fn define_class(&self, name: &str,constructor: napi_callback_raw, properties: PropertiesBuilder)  -> Result<napi_value,NjError> {
        
        let mut js_constructor = ptr::null_mut();
        let mut raw_properties = properties.as_raw_properties();

        debug!("defining class: {} with {} properties",name,raw_properties.len());
        napi_call_result!(
            crate::sys::napi_define_class(
                self.0, 
                name.as_ptr() as *const i8,
                name.len(),
                Some(constructor), 
                ptr::null_mut(), 
                raw_properties.len(), 
                raw_properties.as_mut_ptr(),
                &mut js_constructor
            )
        )?; 
        
        Ok(js_constructor)
    }

    pub fn create_reference(&self, cons: napi_value,count: u32)  -> Result<napi_ref,NjError> {
        
        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_create_reference(
                self.0,
                cons,
                count,
                &mut result
            )
        )?;

        Ok(result)
    }


    pub fn get_new_target(&self, info: napi_callback_info) -> Result<napi_value,NjError> {

        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_get_new_target(
                self.0,
                info,
                &mut result
            )
        )?;

        Ok(result)

    }

    pub fn wrap(&self,js_object: napi_value,rust_obj: *mut u8,finalize: napi_finalize_raw) -> Result<napi_ref,NjError> {
        let mut result = ptr::null_mut();

        napi_call_result!(
            crate::sys::napi_wrap(
                self.0,
                js_object,
                rust_obj as *mut core::ffi::c_void,
                Some(finalize),
                ptr::null_mut(),
                &mut result
            )
        )?;

        Ok(result)
    }

    pub fn unwrap<T>(&self,js_this: napi_value) -> Result<&'static mut T,NjError> {

        let mut result: *mut ::std::os::raw::c_void = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_unwrap(
                self.0,
                js_this,
                &mut result
            )
        )?;

        Ok(unsafe { 
            let rust_ref: &mut T  = &mut * (result as *mut T);
            rust_ref
        })   
    }

    pub fn new_instance(&self, constructor: napi_value,mut args: Vec<napi_value>) -> Result<napi_value,NjError> {

        trace!("napi new instance: {}",args.len());
        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_new_instance(
                self.0,
                constructor,
                args.len(),
                args.as_mut_ptr(),
                &mut result
            )
        )?;

        Ok(result)
    }

    pub fn get_reference_value(&self,obj_ref: napi_ref)  -> Result<napi_value,NjError> {
        
        let mut result = ptr::null_mut();
        napi_call_result!(
            crate::sys::napi_get_reference_value(
                self.0,
                obj_ref,
                &mut result
            )
        )?;

        Ok(result)
    }

    /// create promise and deferred
    pub fn create_promise(&self) -> Result<(napi_value,napi_deferred),NjError> {

        let mut deferred = ptr::null_mut();
        let mut promise = ptr::null_mut();

        napi_call_result!(
            crate::sys::napi_create_promise(
                self.0,
                &mut deferred,
                &mut promise
            )
        )?;

        Ok((promise,deferred))
    }

    pub fn resolve_deferred(&self, deferred: napi_deferred,resolution: napi_value) -> Result<(),NjError> {

        napi_call_result!(
            crate::sys::napi_resolve_deferred(
                self.0,
                deferred,
                resolution
            )
        )
    }

    pub fn reject_deferred(&self, deferred: napi_deferred,rejection: napi_value) -> Result<(),NjError> {

        napi_call_result!(
            crate::sys::napi_reject_deferred(
                self.0,
                deferred,
                rejection
            )
        )
    }


    pub fn create_thread_safe_function (
        &self, 
        name: &str, 
        js_func: Option<napi_value>,
        call_js_cb: napi_threadsafe_function_call_js) -> Result<crate::ThreadSafeFunction, NjError>  {

        use crate::sys::napi_create_threadsafe_function;

        let work_name = self.create_string_utf8(name)?;

        let mut tsfn = ptr::null_mut();

        napi_call_result!(
            napi_create_threadsafe_function(
                self.inner(),
                js_func.unwrap_or(ptr::null_mut()),
                ptr::null_mut(),
                work_name,
                0,
                1,
                ptr::null_mut(),
                None,
                ptr::null_mut(),
                call_js_cb,
                &mut tsfn
            )
        )?;

        Ok(crate::ThreadSafeFunction::new(self.0,tsfn))

    }

    pub fn is_exception_pending(&self) -> bool {

        let mut pending = false;        
        napi_call_assert!(
            crate::sys::napi_is_exception_pending(
                self.inner(),
                &mut pending
            )
        );
        pending
    }

    pub fn throw_type_error(&self,message: &str)  {

        debug!("throwing type error: {}",message);
        // check if there is exception pending, if so log and not do anything
        if self.is_exception_pending() {
            error!("there is exception pending when trying to throw {}, ignoring for now",message);
            return;
        }

        let c_error_msg = CString::new(message).expect("message should not contain null");
        unsafe {
            crate::sys::napi_throw_type_error(
                self.inner(),
                ptr::null_mut(),
                c_error_msg.as_ptr()
            )
        };

    }

}

pub trait JSValue: Sized {

    const JS_TYPE: u32;

    fn convert_to_rust(env: &JsEnv,js_value: napi_value) -> Result<Self,NjError>;
}

impl JSValue for f64 {

    const JS_TYPE: u32 = crate::sys::napi_valuetype_napi_number;

    fn convert_to_rust(env: &JsEnv,js_value: napi_value) -> Result<Self,NjError> {
        let mut value: f64 = 0.0;

        napi_call_result!(
            crate::sys::napi_get_value_double(env.inner(),js_value, &mut value)
        )?;

        Ok(value)
    }
}

impl JSValue for i32 {
    
    const JS_TYPE: u32 = crate::sys::napi_valuetype_napi_number;

    fn convert_to_rust(env: &JsEnv,js_value: napi_value) -> Result<Self,NjError> {
        let mut value: i32 = 0;

        napi_call_result!(
            crate::sys::napi_get_value_int32(env.inner(),js_value, &mut value)
        )?;

        Ok(value)
    }
}



impl JSValue for String {

    const JS_TYPE: u32 = crate::sys::napi_valuetype_napi_string;

    fn convert_to_rust(env: &JsEnv,js_value: napi_value) -> Result<Self,NjError> {

        use crate::sys::napi_get_value_string_utf8;

        let mut chars: [u8; 1024] = [0;1024];
        let mut size: size_t = 0;

        napi_call_result!(
            napi_get_value_string_utf8(env.inner(),js_value,chars.as_mut_ptr() as *mut i8,1024,&mut size)
        )?;

        let my_chars: Vec<u8> = chars[0..size].into();

        String::from_utf8(my_chars).map_err(|err| err.into())
    }


}

#[derive(Clone)]
pub struct JsCallback {
    env:  JsEnv,
    this: napi_value,
    args: Vec<napi_value>
}
   
unsafe impl Send for JsCallback{}
unsafe impl Sync for JsCallback{}

impl JsCallback  {

    pub fn env(&self) -> &JsEnv {
        &self.env
    }

    pub fn args(&self,index: usize) -> napi_value {
        self.args[index]
    }

    pub fn this(&self) -> napi_value {
        self.this
    }

    pub fn this_owned(self) -> napi_value {
        self.this
    }

    /// get value of callback info and verify type
    pub fn get_value<T>(&self, index: usize) -> Result<T,NjError>
        where T: JSValue 
    {
        use crate::sys::napi_typeof;

        let mut valuetype: napi_valuetype = 0;
  
        trace!("get value: {},len: {}",index,self.args.len());

        if index >= self.args.len() {
            debug!("attempt to access callback: {} len: {}",index,self.args.len());
            return Err(NjError::InvalidArgIndex(index,self.args.len()))
        }

        napi_call_result!(
            napi_typeof(
                self.env.inner(),
                self.args[index],
                &mut valuetype
            ))?;

        if  valuetype != T::JS_TYPE {
            debug!("value type is: {} but should be: {}",valuetype,T::JS_TYPE);
            return Err(NjError::InvalidType)
        }

        
        T::convert_to_rust(&self.env, self.args[index])
    }


    pub fn create_thread_safe_function(
        &self, 
        name: &str, 
        index: usize, 
        call_js_cb: napi_threadsafe_function_call_js) -> Result<crate::ThreadSafeFunction,NjError> {

        self.env.create_thread_safe_function(
            name,
            Some(self.args[index]),
            call_js_cb
        )

    }


    pub fn unwrap<T>(&self) -> Result<&'static mut T,NjError>  {
        Ok(self.env.unwrap::<JSObjectWrapper<T>>(self.this())?.mut_inner())
    }

    
}


pub struct JsExports {
    inner: napi_value,
    env: JsEnv
}

impl JsExports {

    pub fn new(env: napi_env,exports: napi_value) -> Self {
        Self {
            inner: exports,
            env: JsEnv::new(env)
        }
    }

    pub fn env(&self) -> &JsEnv {
        &self.env
    }

    pub fn prop_builder(&self) -> PropertiesBuilder {
        PropertiesBuilder::new()
    }


    pub fn define_property(&self, properties: PropertiesBuilder ) -> Result<(),NjError> {
       
        // it is important not to release properties until this call is executed
        // since it is source of name string
        let mut raw_properties = properties.as_raw_properties();

        napi_call_result!(
            crate::sys::napi_define_properties(
                self.env.inner(), 
                self.inner , 
                raw_properties.len(),
                raw_properties.as_mut_ptr()
            )
        )
        
    }

    pub fn set_name_property(&self,name: &str, js_class: napi_value)  -> Result<(),NjError> {
        
        let c_name = CString::new(name).expect("should work");

        napi_call_result!(
            crate::sys::napi_set_named_property(
                self.env.inner(),
                self.inner,
                c_name.as_ptr(),
                js_class
            )
        )

    }
  
}


pub struct JsCallbackFunction {
    ctx: napi_value,
    js_func: napi_value
}



impl JSValue for JsCallbackFunction {
    
    const JS_TYPE: u32 = crate::sys::napi_valuetype_napi_function;

    fn convert_to_rust(env: &JsEnv,js_value: napi_value) -> Result<Self,NjError> {
        
        let ctx = env.get_global()?;
        Ok(Self {
            ctx,
            js_func: js_value
        })
    }
}

impl JsCallbackFunction {

    pub fn call(&self, argv: Vec<napi_value>, env: &JsEnv) -> Result<napi_value,NjError> {
        env.call_function(self.ctx, self.js_func,argv)
    }
}

