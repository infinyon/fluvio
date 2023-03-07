use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::process;
use std::sync::Arc;
use std::os::unix::io::AsRawFd;

use futures_util::StreamExt;
use async_trait::async_trait;
use tracing::{instrument, debug, error, info};
use anyhow::Result;

use fluvio_future::net::{TcpListener, TcpStream};
use fluvio_future::task::spawn;
use fluvio_protocol::api::ApiMessage;
use fluvio_protocol::Decoder as FluvioDecoder;
use fluvio_socket::{FluvioSocket};
use fluvio_types::event::StickyEvent;

pub struct ConnectInfo {
    peer: String,
}

impl fmt::Debug for ConnectInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("peer").field(&self.peer).finish()
    }
}

/// Trait for responding to kf service
/// Request -> Response is type specific
/// Each response is responsible for sending back to socket
#[async_trait]
pub trait FluvioService {
    type Request;
    type Context;

    /// respond to request
    async fn respond(
        self: Arc<Self>,
        context: Self::Context,
        socket: FluvioSocket,
        connection: ConnectInfo,
    ) -> Result<()>;
}

/// Transform Service into Futures 01
pub struct FluvioApiServer<R, A, C, S> {
    req: PhantomData<R>,
    api: PhantomData<A>,
    context: C,
    service: Arc<S>,
    addr: String,
}

impl<R, A, C, S> fmt::Debug for FluvioApiServer<R, A, C, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("FluvioApiServer").field(&self.addr).finish()
    }
}

impl<R, A, C, S> FluvioApiServer<R, A, C, S>
where
    C: Clone,
{
    pub fn new(addr: String, context: C, service: S) -> Self {
        FluvioApiServer {
            req: PhantomData,
            api: PhantomData,
            service: Arc::new(service),
            context,
            addr,
        }
    }
}

impl<R, A, C, S> FluvioApiServer<R, A, C, S>
where
    R: ApiMessage<ApiKey = A> + Send + Debug + 'static,
    C: Clone + Sync + Send + Debug + 'static,
    A: Send + FluvioDecoder + Debug + 'static,
    S: FluvioService<Request = R, Context = C> + Send + Sync + Debug + 'static,
{
    pub fn run(self) -> Arc<StickyEvent> {
        let shutdown = StickyEvent::shared();
        spawn(self.accept_incoming(shutdown.clone()));
        shutdown
    }

    #[instrument(skip(shutdown))]
    async fn accept_incoming(self, shutdown: Arc<StickyEvent>) {
        debug!("Binding TcpListener");
        let listener = match TcpListener::bind(&self.addr).await {
            Ok(listener) => listener,
            Err(err) => {
                error!("Error binding TcpListener: {}", err);
                process::exit(-1);
            }
        };

        info!("Opened TcpListener, waiting for connections");
        let mut incoming = listener.incoming().take_until(shutdown.listen_pinned());

        // Accept incoming connections until None, i.e. terminate has triggered
        while let Some(incoming) = incoming.next().await {
            match incoming {
                Ok(stream) => {
                    info!("Received connection, spawning request handler");
                    let context = self.context.clone();
                    let service = self.service.clone();
                    let host = self.addr.clone();
                    spawn(Self::handle_request(stream, context, service, host));
                }
                Err(e) => {
                    error!("Error from TCP Stream: {:?}", e);
                }
            }
        }

        info!("Closed TcpListener");
    }

    #[instrument(skip(stream, context, service))]
    async fn handle_request(stream: TcpStream, context: C, service: Arc<S>, host: String) {
        let peer_addr = stream
            .peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "".to_owned());
        debug!(%peer_addr, "Handling request");

        let socket = {
            let fd = stream.as_raw_fd();
            FluvioSocket::from_stream(Box::new(stream.clone()), Box::new(stream), fd)
        };

        let connection_info = ConnectInfo {
            peer: peer_addr.clone(),
        };

        let result = service.respond(context, socket, connection_info).await;
        match result {
            Ok(_) => {
                info!(%host, %peer_addr, "Response sent successfully, closing connection");
            }
            Err(err) => {
                error!(%host, %peer_addr, "Error handling stream: {}", err);
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tracing::debug;
    use tracing::trace;

    use fluvio_future::timer::sleep;
    use fluvio_protocol::api::RequestMessage;
    use fluvio_socket::FluvioSocket;

    use crate::test_request::EchoRequest;
    use crate::test_request::SharedTestContext;
    use crate::test_request::TestApiRequest;
    use crate::test_request::TestContext;
    use crate::test_request::TestKafkaApiEnum;
    use crate::test_request::TestService;

    use super::*;

    fn create_server(
        addr: String,
    ) -> FluvioApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> {
        let ctx = Arc::new(TestContext::new());
        let server: FluvioApiServer<
            TestApiRequest,
            TestKafkaApiEnum,
            SharedTestContext,
            TestService,
        > = FluvioApiServer::new(addr, ctx, TestService::new());

        server
    }

    async fn create_client(addr: String) -> FluvioSocket {
        debug!("client wait for 1 second for 2nd server to come up");
        sleep(Duration::from_millis(200)).await;
        FluvioSocket::connect(&addr).await.expect("connect failed")
    }

    async fn test_client_sync_requests(addr: String) {
        let mut socket = create_client(addr).await;

        let request = EchoRequest::new("hello".to_owned());
        let msg = RequestMessage::new_request(request);
        let reply = socket.send(&msg).await.expect("send");
        trace!("received reply from server: {:#?}", reply);
        assert_eq!(reply.response.msg, "hello");

        // send 2nd message on same socket
        let request2 = EchoRequest::new("hello2".to_owned());
        let msg2 = RequestMessage::new_request(request2);
        let reply2 = socket.send(&msg2).await.expect("send");
        trace!("received 2nd reply from server: {:#?}", reply2);
        assert_eq!(reply2.response.msg, "hello2");
    }

    // send 2 requests and drop socket
    async fn test_client_async_requests(addr: String) {
        let mut socket = create_client(addr).await;

        let request = EchoRequest::new("hello".to_owned());
        let msg = RequestMessage::new_request(request);
        socket
            .get_mut_sink()
            .send_request(&msg)
            .await
            .expect("send");

        let request2 = EchoRequest::new("hello2".to_owned());
        let msg2 = RequestMessage::new_request(request2);
        socket
            .get_mut_sink()
            .send_request(&msg2)
            .await
            .expect("send");
    }

    #[fluvio_future::test(ignore)]
    async fn test_server() {
        // create fake server, anything will do since we only
        // care about creating tcp stream

        let port = portpicker::pick_unused_port().expect("No free ports left");
        let socket_addr = format!("127.0.0.1:{port}");

        let server = create_server(socket_addr.clone());
        let service = server.service.clone();
        let shutdown = server.run();
        test_client_async_requests(socket_addr.clone()).await;

        test_client_sync_requests(socket_addr.clone()).await;
        assert_eq!(service.processed_requests.load(Ordering::SeqCst), 4);
        shutdown.notify();
    }
}
