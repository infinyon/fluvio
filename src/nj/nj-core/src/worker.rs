use std::ptr;

use log::error;
use async_trait::async_trait;
use futures::Future;

use flv_future_core::spawn;

use crate::sys::napi_deferred;
use crate::sys::napi_value;
use crate::val::JsEnv;
use crate::NjError;
use crate::sys::napi_env;
use crate::sys::napi_callback_info;
use crate::TryIntoJs;
use crate::IntoJs;
use crate::assert_napi;
use crate::ThreadSafeFunction;

pub struct JsFuture<F>{
    future: F,
    name: String
}

impl <F> JsFuture<F> 
    where F: Future, F::Output: TryIntoJs
{

    pub fn new<S>(future: F,name: S) -> Self
        where S:Into<String>
    {
        Self {
            future,
            name: name.into()
        }
    }
}

impl <F>TryIntoJs for JsFuture<F> 
    where F: Future + 'static + Send , F::Output: TryIntoJs {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        
        create_promise(js_env,&self.name,self.future)
    }
}


struct JsDeferred(napi_deferred);
unsafe impl Send for JsDeferred {}

pub struct WorkerResult<O> {
    deferred: JsDeferred,
    result: O
}

/// create promise and schedule work
/// when this is finished it will return result in the main thread
pub fn create_promise<F,O>(js_env: &JsEnv, name: &str, future: F) -> Result<napi_value, NjError>
where
    F: Future<Output = O> + 'static + Send,
    O: TryIntoJs
{
    let (promise, deferred) = js_env.create_promise()?;
    let function_name = format!("async_worker_th_{}", name);
    let ts_fn =
        js_env.create_thread_safe_function(&function_name, None, Some(promise_complete::<O>))?;
    let js_deferred = JsDeferred(deferred);

    spawn(async move {
        let result = future.await;
        finish_worker(ts_fn, result, js_deferred);
    });

    Ok(promise)
}

extern "C" fn promise_complete<O>(
    env: napi_env,
    _js_cb: napi_value,
    _context: *mut ::std::os::raw::c_void,
    data: *mut ::std::os::raw::c_void,
) where
    O: TryIntoJs
{
    if env != ptr::null_mut() {
        let js_env = JsEnv::new(env);

        let worker_result: Box<WorkerResult<O>> =
            unsafe { Box::from_raw(data as *mut WorkerResult<O>) };

        let result: Result<(), NjError> = (move || 
            match worker_result.result.try_to_js(&js_env) {
                Ok(val) => js_env.resolve_deferred(worker_result.deferred.0, val),
                Err(err) => js_env.reject_deferred(worker_result.deferred.0, err.to_js(&js_env)),
        })();
        assert_napi!(result)
    }
}


fn finish_worker<O>(ts_fn: ThreadSafeFunction, result: O, deferred: JsDeferred)
where
    O: TryIntoJs
{
    let boxed_worker = Box::new(WorkerResult { result, deferred });
    let ptr = Box::into_raw(boxed_worker);
    if let Err(err) = ts_fn.call(Some(ptr as *mut core::ffi::c_void)) {
        error!("error finishing worker: {}", err);
    }
}

#[async_trait]
pub trait JSWorker: Sized + Send + 'static {

    type Output: TryIntoJs;

    /// create new worker based on argument based in the callback
    /// only need if it is called as method
    fn create_worker(_env: &JsEnv, _info: napi_callback_info) -> Result<Self, NjError> {
        Err(NjError::InvalidType)
    }

    /// call by Node to create promise
    #[no_mangle]
    extern "C" fn start_promise(env: napi_env, info: napi_callback_info) -> napi_value {
        let js_env = JsEnv::new(env);

        let result: Result<napi_value, NjError> = (|| {
            let worker = Self::create_worker(&js_env, info)?;
            worker.create_promise(&js_env)
        })();

        result.to_js(&js_env)
    }

    /// create promise and schedule work
    /// when this is finished it will return result in the main thread
    fn create_promise(self, js_env: &JsEnv) -> Result<napi_value, NjError> {
        let (promise, deferred) = js_env.create_promise()?;
        let function_name = format!("async_worker_th_{}", std::any::type_name::<Self>());
        let ts_fn =
            js_env.create_thread_safe_function(&function_name, None, Some(Self::complete))?;
        let js_deferred = JsDeferred(deferred);

        spawn(async move {
            let result = self.execute().await;
            finish_worker(ts_fn, result, js_deferred);
        });

        Ok(promise)
    }

    /// execute this in async worker thread
    async fn execute(mut self) -> Self::Output;

    // call by Node to convert result into JS value
    extern "C" fn complete(
        env: napi_env,
        _js_cb: napi_value,
        _context: *mut ::std::os::raw::c_void,
        data: *mut ::std::os::raw::c_void,
    ) {
        if env != ptr::null_mut() {
            let js_env = JsEnv::new(env);

            let worker_result: Box<WorkerResult<Self::Output>> =
                unsafe { Box::from_raw(data as *mut WorkerResult<Self::Output>) };

            let result: Result<(), NjError> = (move || 
                match worker_result.result.try_to_js(&js_env) {
                    Ok(val) => js_env.resolve_deferred(worker_result.deferred.0,val),
                    Err(err) => js_env.reject_deferred(worker_result.deferred.0, err.to_js(&js_env))
            })();
            assert_napi!(result);
        }
    }
}

