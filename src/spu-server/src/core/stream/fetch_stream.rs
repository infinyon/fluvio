use std::collections::HashMap;
use std::io::Cursor;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::SystemTime;

use bytes::BytesMut;
use chashmap::CHashMap;
use chashmap::ReadGuard;
use futures::future::TryFutureExt;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::debug;
use log::error;
use log::trace;

use types::SpuId;
use future_helper::spawn;
use internal_api::InternalApiRequest;
use internal_api::InternalApiResponse;
use internal_api::InternalKafkaApiEnum;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::Decoder;
use kf_protocol::Encoder;

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

unsafe impl Sync for FollowerInfo {}

#[derive(Debug)]
pub struct FollowerInfo {
    pub spu_id: SpuId,
    pub last_time: SystemTime,
    pub ack_count: u16,
}

impl FollowerInfo {}

// split followers info and sink so there is less lock contention
#[derive(Debug)]
pub struct SpuStreams {
    followers_info: CHashMap<SpuId, FollowerInfo>,
    sinks: CHashMap<SpuId, KfTcpStreamSplitSink>,
    followers: RwLock<HashMap<SpuId, bool>>,
}

impl SpuStreams {
    pub fn new() -> Self {
        Self {
            followers_info: CHashMap::new(),
            sinks: CHashMap::new(),
            followers: RwLock::new(HashMap::new()),
        }
    }

    async fn watch_changes(
        self: Arc<Self>,
        spu_id: SpuId,
        mut stream: KfTcpStreamSplitStream,
    ) -> Result<(), IoError> {
        debug!("fetch stream: starting watching changes: {}", spu_id);
        while let Some(result) = stream.next().await {
            let bytes = result?;
            debug!(
                "fetch stream: received from spu: {}, ack bytes: {} ",
                spu_id,
                bytes.len()
            );
            {
                let follower_info = self.followers_info.get_mut(&spu_id);
                debug!("fetch stream: retrieved follower info");
                if let Some(mut follower_info) = follower_info {
                    follower_info.last_time = SystemTime::now();
                    follower_info.ack_count = follower_info.ack_count + 1;
                    trace!(
                        "fetch stream: follower: {}, ack: {}",
                        spu_id,
                        follower_info.ack_count
                    );

                } else {
                    error!(
                        "fetch stream: got ack from follower doesn't exist anymore: {}, finishing",
                        spu_id
                    );
                    return Ok(());
                }
            }

            trace!("fetch stream: finishing ack. waiting for next client request");
        }

        debug!("fetch stream: follower stream terminated. finishing");

        Ok(())
    }

    #[allow(dead_code)]
    fn handle_request_from_follower(&self, req_bytes: BytesMut) -> Result<(), IoError> {
        let mut src = Cursor::new(&req_bytes);
        let req_msg: RequestMessage<InternalApiRequest, InternalKafkaApiEnum> =
            RequestMessage::decode_from(&mut src)?;

        match req_msg.request {
            InternalApiRequest::FetchAckRequest(_req) => {
                debug!("got ping request");
            }
            _ => error!("unreg request: {:#?}", req_msg),
        }

        Ok(())
    }

    // add follower stream for spu
    pub fn add(self: Arc<Self>, spu_id: SpuId, split: KfTcpStreamSplit) {
        debug!("adding fetch stream spu: {}", spu_id);
        let (sink, stream) = split.as_tuple();

        let info = FollowerInfo {
            spu_id,
            last_time: SystemTime::now(),
            ack_count: 0,
        };

        let mut follower = self
            .followers
            .write()
            .expect("follower lock must always lock");
        follower.insert(spu_id, true);
        self.followers_info.insert(spu_id, info);
        self.sinks.insert(spu_id, sink);

        let my_clone = self.clone();
        spawn(my_clone.watch_changes(spu_id, stream).map_err(|err| {
            error!("fetch stream: error watching streams: {}", err);
            ()
        }));
    }

    pub fn get_followers_info(&self, spu_id: &SpuId) -> Option<ReadGuard<SpuId, FollowerInfo>> {
        self.followers_info.get(spu_id)
    }

    // send to internal sink
    pub async fn send_request(
        self: Arc<Self>,
        spu_id: SpuId,
        response: ResponseMessage<InternalApiResponse, InternalKafkaApiEnum>,
    ) -> Result<(), StreamError> {
        if let Some(mut sink) = self.sinks.get_mut(&spu_id) {
            debug!("fetch stream: sending response to spu: {}", spu_id);
            sink.send(response.as_bytes()?).await?;
            trace!("fetch stream: sending response to spu finished");
            Ok(())
        } else {
            Err(StreamError::NoFollower)
        }
    }
}

#[cfg(test)]
mod test {

    use std::io::Error as IoError;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::FutureExt;
    use futures::stream::StreamExt;
    use log::debug;

    use types::SpuId;
    use future_aio::net::AsyncTcpListener;
    use future_aio::net::AsyncTcpStream;
    use future_helper::sleep;
    use future_helper::test_async;

    use super::SpuStreams;
    use crate::KfTcpStreamSplit;

    /// create server and create client stream
    async fn create_server(listener: AsyncTcpListener, client_count: u16) -> Result<(), IoError> {
        debug!("server: successfully binding. waiting for incoming");
        let mut incoming = listener.incoming();
        let mut count = 0;
        let mut tcp_streams: Vec<AsyncTcpStream> = Vec::new();
        while let Some(stream) = incoming.next().await {
            debug!("server: got connection from client: {}", count);
            let tcp_stream = stream?;
            tcp_streams.push(tcp_stream);
            yield;
            count = count + 1;
            if count >= client_count {
                break;
            }
        }

        debug!("server: sleeping for 1 second to give client chances");
        sleep(Duration::from_micros(1000)).await.expect("panic");
        Ok(()) as Result<(), IoError>
    }

    #[test_async]
    async fn test_stream_add() -> Result<(), IoError> {
        //utils::init_logger();

        let count = 5;

        let bk_stream = Arc::new(SpuStreams::new());

        // create fake server, anything will do since we only
        // care creating tcp stream
        let addr = "127.0.0.1:29998".parse::<SocketAddr>().expect("parse");
        let listener = AsyncTcpListener::bind(&addr)?;

        let server_ft = create_server(listener, count);

        let client_ft = async {
            debug!("client: sleep to give server chance to come up");
            for i in 0..count {
                debug!("client: trying to connect {}", i);
                let tcp_stream = AsyncTcpStream::connect(&addr).await?;
                debug!("client: connected, adding to bk stream: {}", i);
                let split: KfTcpStreamSplit = tcp_stream.split();
                bk_stream.clone().add(i as SpuId, split);
            }

            debug!("client sleeping for second to give server chance to catch up");
            sleep(Duration::from_micros(200)).await.expect("panic");

            Ok(()) as Result<(), IoError>
        };

        client_ft.join(server_ft).await;

        Ok(())
    }

}
