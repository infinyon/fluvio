/// maintain connection to leader
use std::io::Error as IoError;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::RwLock;
use std::collections::HashMap;

use log::error;
use log::debug;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::future::TryFutureExt;
use chashmap::CHashMap;
use chashmap::WriteGuard;

use flv_types::SpuId;
use kf_protocol::Encoder;
use kf_protocol::Decoder;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use internal_api::InternalApiResponse;
use internal_api::InternalApiRequest;
use internal_api::InternalKafkaApiEnum;
use internal_api::FetchAckResponse;
use flv_future_core::spawn;

use crate::KfTcpStreamSplit;
use crate::KfTcpStreamSplitSink;
use crate::KfTcpStreamSplitStream;

pub enum StreamError {
    #[allow(dead_code)]
    NoFollower,
    IoError(IoError),
}

impl From<IoError> for StreamError {
    fn from(error: IoError) -> Self {
        StreamError::IoError(error)
    }
}

// split followers info and sink so there is less lock contention
#[derive(Debug)]
pub struct LeaderStreams {
    sinks: CHashMap<SpuId, KfTcpStreamSplitSink>,
    leaders: RwLock<HashMap<SpuId, bool>>,
}

impl LeaderStreams {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            sinks: CHashMap::new(),
            leaders: RwLock::new(HashMap::new()),
        }
    }

    /// watch changes in the stream. in this case, we receive the batch request,
    /// first we will send ack, then save the batch
    #[allow(dead_code)]
    async fn watch_changes(
        self: Arc<Self>,
        leader_id: SpuId,
        mut stream: KfTcpStreamSplitStream,
    ) -> Result<(), IoError> {
        debug!("fetch stream: starting watching changes: {}", leader_id);
        while let Some(result) = stream.next().await {
            let bytes = result?;

            debug!(
                "fetch stream: received from spu: {},ack bytes: {} ",
                leader_id,
                bytes.len()
            );
            // decode request
            let mut src = Cursor::new(&bytes);
            let _request =
                RequestMessage::<InternalApiRequest, InternalKafkaApiEnum>::decode_from(&mut src);

            if let Some(sink) = self.sinks.get_mut(&leader_id) {
                debug!("sending ack back to response: {}", leader_id);
                if let Err(err) = send_ack(leader_id, sink).await {
                    error!("error sending ack back: {}", err);
                }
            } else {
                error!("sink for leader not found: {}", leader_id);
            }
        }

        debug!("fetch stream: follower stream terminated. finishing");

        Ok(())
    }

    // add stream to connect to leader
    #[allow(dead_code)]
    pub fn add(self: Arc<Self>, leader_id: SpuId, split: KfTcpStreamSplit) {
        debug!("adding fetch stream spu: {}", leader_id);
        let (sink, stream) = split.as_tuple();

        let mut leader = self.leaders.write().expect("leader lock must always lock");
        leader.insert(leader_id, true);
        self.sinks.insert(leader_id, sink);

        let my_clone = self.clone();
        spawn(my_clone.watch_changes(leader_id, stream).map_err(|err| {
            error!("fetch stream: error watching streams: {}", err);
            ()
        }));
    }
}

#[allow(dead_code)]
async fn send_ack(
    leader_id: SpuId,
    mut sink: WriteGuard<SpuId, KfTcpStreamSplitSink>,
) -> Result<(), IoError> {
    let mut ack_message = ResponseMessage::<InternalApiResponse, InternalKafkaApiEnum>::default();
    let mut response = FetchAckResponse::default();
    response.spu_id = leader_id;
    ack_message.response = InternalApiResponse::FetchAckResponse(response);

    debug!("sending ack back to response: {}", leader_id);
    sink.send(ack_message.as_bytes()?).await
}
