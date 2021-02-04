use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use chashmap::CHashMap;
use chashmap::WriteGuard;
use tracing::trace;

use crate::KfSink;

pub type SharedSinkPool<T> = Arc<SinkPool<T>>;

/// Pool of sinks.  This is lightweight version of SocketPool
/// where you only need to keep track of sink
/// no attemp to keep id indexes
#[derive(Debug)]
pub struct SinkPool<T>(CHashMap<T, KfSink>);

impl<T> SinkPool<T>
where
    T: Eq + PartialEq + Hash + Debug + Clone,
    KfSink: Sync,
{
    pub fn new_shared() -> SharedSinkPool<T> {
        Arc::new(Self::new())
    }
    pub fn new() -> Self {
        Self(CHashMap::new())
    }

    pub fn insert_sink(&self, id: T, socket: KfSink) {
        trace!("inserting sink at: {:#?}", id);
        self.0.insert(id, socket);
    }

    pub fn clear_sink(&self, id: &T) {
        self.0.remove(id);
    }

    /// get sink
    pub fn get_sink<'a>(&'a self, id: &T) -> Option<WriteGuard<'a, T, KfSink>> {
        self.0.get_mut(id)
    }
}

impl<T> Default for SinkPool<T>
where
    T: Eq + PartialEq + Hash + Debug + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use futures_util::future::join;
    use futures_util::stream::StreamExt;
    use tracing::debug;
    use tracing::info;

    use fluvio_future::net::TcpListener;
    use fluvio_future::test_async;
    use fluvio_future::timer::sleep;
    use fluvio_protocol::api::RequestMessage;

    use super::SinkPool;
    use crate::test_request::EchoRequest;
    use crate::test_request::EchoResponse;
    use crate::test_request::TestApiRequest;
    use crate::test_request::TestKafkaApiEnum;
    use crate::FlvSocket;
    use crate::FlvSocketError;

    async fn test_server(addr: &str) -> Result<(), FlvSocketError> {
        let sink_pool: SinkPool<u16> = SinkPool::new();

        let listener = TcpListener::bind(addr).await?;
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let socket: FlvSocket = incoming_stream.into();

        let (sink, mut stream) = socket.split();
        let id: u16 = 0;
        sink_pool.insert_sink(id, sink);
        let mut api_stream = stream.api_stream::<TestApiRequest, TestKafkaApiEnum>();

        let msg = api_stream.next().await.expect("msg").expect("unwrap");
        debug!("msg received: {:#?}", msg);
        match msg {
            TestApiRequest::EchoRequest(echo_request) => {
                let resp = echo_request.new_response(EchoResponse::new("yes".to_owned()));
                let mut sink = sink_pool.get_sink(&id).expect("sink");
                sink.send_response(&resp, 0).await.expect("send succeed");

                // can't detect sink failures
                let resp2 = echo_request.new_response(EchoResponse::new("yes2".to_owned()));
                sink.send_response(&resp2, 0)
                    .await
                    .expect("error should occur");

                // can detect api stream end
                /*
                match api_stream.next().await {
                    Some(_) => assert!(false,"should not received"),
                    None => assert!(true,"none")
                }
                */
            }
            _ => panic!("no echo request"),
        }

        debug!("server: finish sending out");
        Ok(())
    }

    async fn setup_client(addr: &str) -> Result<(), FlvSocketError> {
        sleep(Duration::from_millis(20)).await;
        debug!("client: trying to connect");
        let mut socket = FlvSocket::connect(&addr).await?;
        info!("client: connect to test server and waiting...");

        let request = RequestMessage::new_request(EchoRequest::new("hello".to_owned()));
        socket.send(&request).await.expect("send success");
        Ok(())
    }

    #[test_async]
    async fn test_sink_pool() -> Result<(), FlvSocketError> {
        let addr = "127.0.0.1:5999";

        let _r = join(setup_client(addr), test_server(addr)).await;
        Ok(())
    }
}
