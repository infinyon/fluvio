// JS Wrapper for SpuLeader

use std::sync::Arc;

use log::debug;
use futures::stream::StreamExt;

use flv_client::SpuReplicaLeader;
use flv_client::ReplicaLeader;
use flv_future_aio::sync::RwLock;
use flv_future_aio::task::spawn;
use flv_client::ClientError;
use flv_client::FetchLogOption;
use flv_client::FetchOffset;

use node_bindgen::derive::node_bindgen;
use node_bindgen::core::NjError;
use node_bindgen::core::val::JsEnv;
use node_bindgen::core::TryIntoJs;
use node_bindgen::sys::napi_value;
use node_bindgen::core::JSClass;
use node_bindgen::core::val::JsObject;

type SharedReplicaLeader = Arc<RwLock<SpuReplicaLeader>>;
pub struct ReplicaLeaderWrapper(SpuReplicaLeader);

impl From<SpuReplicaLeader> for ReplicaLeaderWrapper {
    fn from(leader: SpuReplicaLeader) -> Self {
        Self(leader)
    }
}

impl TryIntoJs for ReplicaLeaderWrapper {
    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value, NjError> {
        let new_instance = JsReplicaLeader::new_instance(js_env, vec![])?;
        JsReplicaLeader::unwrap_mut(js_env, new_instance)?.set_leader(self.0);
        Ok(new_instance)
    }
}

pub struct JsReplicaLeader {
    inner: Option<SharedReplicaLeader>,
    option: FetchLogOption,
}

#[node_bindgen]
impl JsReplicaLeader {
    #[node_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: None,
            option: FetchLogOption::default(),
        }
    }

    pub fn set_leader(&mut self, leader: SpuReplicaLeader) {
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

    /// consume message from replica
    /// first argument if offset which can be variation of
    ///     string:     'earliest','latest'
    ///     number:     offset
    ///     param:      {
    ///                     offset: <offset>,
    ///                     isolation:  'read_uncommitted','read_committed'
    ///                     max_bytes: number
    ///                 }
    ///
    /// example:
    ///     leader.consume("earliest",emitter.emit.bind(emitter));
    ///     leader.consume("latest",emitter.emit.bind(emitter));
    ///     leader.consume(2,emitter.emit.bind(emitter));
    ///     leader.consume({
    ///           offset: "earliest",
    ///           includeMetadata: true,
    ///           type: 'text',
    ///           isolation: 'readCommitted'
    ///      });
    ///
    #[node_bindgen(mt)]
    fn consume<F: Fn(String, String) + 'static + Send + Sync>(
        &self,
        offset_option: JsObject,
        cb: F,
    ) -> Result<(), NjError> {
        debug!("consume, checking to see offset is");
        // check if we can convert into string
        let offset = if let Ok(offset_string) = offset_option.as_value::<String>() {
            debug!("received offset string: {}", offset_string);
            offset_from_string(offset_string)?
        } else if let Ok(offset_number) = offset_option.as_value::<i64>() {
            FetchOffset::Offset(offset_number)
        } else if let Ok(js_config) = offset_option.as_value::<JsObject>() {
            // get offset
            let offset_property = js_config.get_property("offset")?;
            debug!("offset property found");
            if let Ok(offset_string) = offset_property.as_value::<String>() {
                offset_from_string(offset_string)?
            } else if let Ok(offset_number) = offset_property.as_value::<i64>() {
                FetchOffset::Offset(offset_number)
            } else {
                return Err(NjError::Other("unrecognized offset arg".to_owned()))
            }
           
        } else {
            // check if this is object
            return Err(NjError::Other("unrecognized offset arg".to_owned()));
        };

        let leader = self.inner.as_ref().unwrap().clone();
        debug!("starting inner consume");
        spawn(consume_inner(leader, offset, self.option.clone(), cb));

        Ok(())
    }
}

// perform async fetching of stream and send back to JS callback
async fn consume_inner<F: Fn(String, String)>(
    leader: SharedReplicaLeader,
    offset: FetchOffset,
    option: FetchLogOption,
    cb: F,
) -> Result<(), NjError> {
    let event = "data".to_owned();

    let mut leader_w = leader.write().await;

    debug!("getting fetch log stream");

    let mut log_stream = leader_w.fetch_logs(offset, option);

    debug!("find log stream");

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

    Ok(())
}

/// convert from string to fetch offset
fn offset_from_string(value: String) -> Result<FetchOffset, NjError> {
    if value == "earliest" {
        Ok(FetchOffset::Earliest)
    } else if value == "latest" {
        Ok(FetchOffset::Latest)
    } else {
        return Err(NjError::Other(format!(
            "invalid option: {}, valid values are: earliest/latest",
            value
        )));
    }
}


