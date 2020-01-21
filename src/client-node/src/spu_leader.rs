// JS Wrapper for SpuLeader
use std::ptr;
use std::sync::Arc;

use log::debug;
use futures::stream::StreamExt;
use flv_client::SpuLeader;
use flv_client::ReplicaLeader;
use flv_future_aio::sync::RwLock;
use flv_future_core::spawn;
use nj::core::JSClass;
use nj::core::NjError;
use nj::core::val::JsEnv;
use nj::sys::napi_ref;
use nj::sys::napi_value;
use nj::sys::napi_env;
use nj::core::Property;
use nj::core::val::JsCallback;
use nj::sys::napi_callback_info;
use nj::core::PropertiesBuilder;
use nj::core::ToJsValue;
use nj::core::create_promise;
use nj::core::result_to_napi;
use nj::core::assert_napi;
use kf_protocol::api::MAX_BYTES;
use kf_protocol::api::Isolation;
use kf_protocol::api::PartitionOffset;
use kf_protocol::api::DefaultRecords;

use crate::JsClientError;

type SharedSpuLeader = Arc<RwLock<SpuLeader>>;
pub struct SpuLeaderWrapper(SpuLeader);

impl From<SpuLeader> for SpuLeaderWrapper {
    fn from(leader: SpuLeader) -> Self {
        Self(leader)
    }
}


impl ToJsValue for SpuLeaderWrapper {

    fn to_js(self, js_env: &JsEnv) -> napi_value {

        let new_instance = result_to_napi!(JsSpuLeader::new_instance(js_env,vec![]),js_env);
        result_to_napi!(JsSpuLeader::unwrap(js_env,new_instance),js_env).set_leader(self.0);
        new_instance

    }
}


/// Reference to JavaScript Constructor
static mut JS_SPU_LEADER_CONSTRUCTOR: napi_ref = ptr::null_mut();

pub struct JsSpuLeader {
    inner: Option<SharedSpuLeader>
}


impl JsSpuLeader {

    pub fn new() -> Self {
        Self {
            inner: None
        }
    }

    pub fn set_leader(&mut self , leader: SpuLeader) {
        self.inner.replace(Arc::new(RwLock::new(leader)));
    }

    /// send string to replica
    /// produce (message)
    #[no_mangle]
    pub extern "C" fn js_produce(env: napi_env, info: napi_callback_info) -> napi_value  {

        let js_env = JsEnv::new(env);
        let js_cb = result_to_napi!(js_env.get_cb_info(info, 1),&js_env); 
        let message = result_to_napi!(js_cb.get_value::<String>(0),&js_env);
        let js_leader = result_to_napi!(js_cb.unwrap::<Self>(),&js_env);

        if let Some(ref ref_leader) = js_leader.inner {

            let leader = ref_leader.clone();
            result_to_napi!(create_promise(&js_env,"leader_produce_ft",async move {
                let mut producer = leader.write().await;
                let bytes = message.into_bytes();
                let len = bytes.len();
                producer.send_record(bytes).await
                    .map( |_| len as i64 )
                    .map_err( |err| err.into()) as Result<i64,JsClientError>

                }),&js_env)
        } else {
            println!("leader was not initialized properly");
            ptr::null_mut()
        }
    }

    /// consume message from topic
    /// consume(topic,partition,emitter_cb)
    #[no_mangle]
    pub extern "C" fn js_consume(env: napi_env, info: napi_callback_info) -> napi_value  {

        let js_env = JsEnv::new(env);
        let js_cb = result_to_napi!(js_env.get_cb_info(info, 1),&js_env); 
        let emit_fn = js_cb.args(0);

        let js_leader = result_to_napi!(js_cb.unwrap::<Self>(),&js_env);

        let leader = js_leader.inner.as_ref().unwrap().clone();  // there should be always leader
        
        debug!("invoking ts at spu leader");
        let xtsfn = result_to_napi!(js_env.create_thread_safe_function("spu_leader_event",Some(emit_fn),Some(invoke_event_emitter)),&js_env);

        spawn( async move {

            let mut leader_w = leader.write().await;
            let offsets = leader_w.fetch_offsets().await.expect("offset should not fail");
            let beginning_offset = offsets.start_offset();

            let mut log_stream = leader_w.fetch_logs(
                beginning_offset, 
                MAX_BYTES, 
                Isolation::ReadCommitted);

            while let Some(partition_response) = log_stream.next().await {

                let records = partition_response.records;
                let value = Box::new(StreamValue {
                    records
                });
                let raw_ptr = Box::into_raw(value);
                xtsfn.call(Some(raw_ptr as *mut core::ffi::c_void)).expect("js callback should work");
        
            }
        });
   
        ptr::null_mut()
    }

}



impl JSClass for JsSpuLeader {

    const CLASS_NAME: &'static str = "SpuLeader";

    const CONSTRUCTOR_ARG_COUNT: usize = 0;

    fn create_from_js(_js_cb: &JsCallback) -> Result<Self, NjError> {

        debug!("creating Spu Leader");

        Ok(Self::new())
    }

    fn set_constructor(constructor: napi_ref) {
        unsafe {
            JS_SPU_LEADER_CONSTRUCTOR = constructor;
        }
    }

    fn get_constructor() -> napi_ref {
        unsafe { JS_SPU_LEADER_CONSTRUCTOR }
    }


    fn properties() -> PropertiesBuilder {
        vec![
            Property::new("produce").method(Self::js_produce),
            Property::new("consume").method(Self::js_consume)
        ].into()
    }

}


pub struct StreamValue {
    records: DefaultRecords          // default records
}

unsafe impl Send for StreamValue{}

// invoke event emitter
extern "C" fn invoke_event_emitter(
    env: napi_env,
    js_cb: napi_value, 
    _context: *mut ::std::os::raw::c_void,
    data: *mut ::std::os::raw::c_void) {

    if env != ptr::null_mut() {

        let js_env = JsEnv::new(env); 
        let global = assert_napi!(js_env.get_global());
        let on_event = assert_napi!(js_env.create_string_utf8("data"));

        let value: Box<StreamValue> = unsafe { Box::from_raw(data as *mut StreamValue) };

        
        for batch in value.records.batches {
            for record in batch.records {
                if let Some(bytes) = record.value().inner_value() {
                    // construct javascript string
                    let message = assert_napi!(js_env.create_string_utf8_from_bytes(&bytes));
                    assert_napi!(js_env.call_function(global,js_cb,vec![on_event,message]));
                }
            }
            
        }
    }
    
}