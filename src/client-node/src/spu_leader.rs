// JS Wrapper for SpuLeader

use std::sync::Arc;

use log::debug;

use futures::stream::StreamExt;
use flv_client::SpuLeader;
use flv_client::ReplicaLeader;
use flv_future_aio::sync::RwLock;
use flv_client::ClientError;

use kf_protocol::api::MAX_BYTES;
use kf_protocol::api::Isolation;
use kf_protocol::api::PartitionOffset;
use node_bindgen::derive::node_bindgen;
use node_bindgen::core::NjError;
use node_bindgen::core::val::JsEnv;
use node_bindgen::core::TryIntoJs;
use node_bindgen::sys::napi_value;
use node_bindgen::core::JSClass;

type SharedSpuLeader = Arc<RwLock<SpuLeader>>;
pub struct SpuLeaderWrapper(SpuLeader);

impl From<SpuLeader> for SpuLeaderWrapper {
    fn from(leader: SpuLeader) -> Self {
        Self(leader)
    }
}

impl TryIntoJs for SpuLeaderWrapper {
    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value, NjError> {
        let new_instance = JsSpuLeader::new_instance(js_env, vec![])?;
        JsSpuLeader::unwrap(js_env, new_instance)?.set_leader(self.0);
        Ok(new_instance)
    }
}

pub struct JsSpuLeader {
    inner: Option<SharedSpuLeader>,
}

#[node_bindgen]
impl JsSpuLeader {
    #[node_bindgen(constructor)]
    pub fn new() -> Self {
        Self { inner: None }
    }

    pub fn set_leader(&mut self, leader: SpuLeader) {
        self.inner.replace(Arc::new(RwLock::new(leader)));
    }

    /// send string to replica
    /// produce (message)
    #[node_bindgen]
    async fn produce(&self, message: String) -> Result<i64, ClientError> {
        let leader = self.inner.as_ref().unwrap().clone();

        let mut producer = leader.write().await;
        let bytes = message.into_bytes();
        let len = bytes.len();
        producer.send_record(bytes).await.map(|_| len as i64)
    }

    /// consume message from topic
    /// consume(topic,partition,emitter_cb)
    #[node_bindgen]
    async fn consume<F: Fn(String, String)>(&self, cb: F) {
        let leader = self.inner.as_ref().unwrap().clone(); // there should be always leader

        let mut leader_w = leader.write().await;

        let offsets = leader_w
            .fetch_offsets()
            .await
            .expect("offset should not fail");
        let beginning_offset = offsets.start_offset();

        debug!("starting consume with fetch offset: {}", beginning_offset);

        let mut log_stream =
            leader_w.fetch_logs(beginning_offset, MAX_BYTES, Isolation::ReadCommitted);

        let event = "data".to_owned();

        while let Some(partition_response) = log_stream.next().await {
            let records = partition_response.records;

            debug!("received records: {:#?}", records);

            for batch in records.batches {
                for record in batch.records {
                    if let Some(bytes) = record.value().inner_value() {
                        let msg = String::from_utf8(bytes).expect("string");
                        debug!("msg: {}", msg);
                        cb(event.clone(), msg);
                    }
                }
            }
        }
    }
}
