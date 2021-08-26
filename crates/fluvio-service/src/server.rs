use std::fmt;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::marker::PhantomData;
use std::process;
use std::sync::Arc;
use std::os::unix::io::AsRawFd;

use futures_util::StreamExt;
use async_trait::async_trait;
use tracing::{debug, error, info};
use tracing::instrument;
use tracing::trace;

use fluvio_types::event::SimpleEvent;
use fluvio_future::net::{TcpListener, TcpStream};
use fluvio_future::task::spawn;
use fluvio_protocol::api::ApiMessage;
use fluvio_protocol::Decoder as FluvioDecoder;
use fluvio_socket::{FluvioSocket, SocketError};

#[async_trait]
pub trait SocketBuilder: Clone {
    async fn to_socket(&self, raw_stream: TcpStream) -> Result<FluvioSocket, IoError>;
}

#[derive(Debug, Clone)]
pub struct DefaultSocketBuilder {}

#[async_trait]
impl SocketBuilder for DefaultSocketBuilder {
    async fn to_socket(&self, raw_stream: TcpStream) -> Result<FluvioSocket, IoError> {
        let fd = raw_stream.as_raw_fd();
        Ok(FluvioSocket::from_stream(
            Box::new(raw_stream.clone()),
            Box::new(raw_stream),
            fd,
        ))
    }
}


#[derive(Debug)]
pub struct ConnectInfo {
    host: String,
    peer: String
}

/// Trait for responding to kf service
/// Request -> Response is type specific
/// Each response is responsible for sending back to socket
#[async_trait]
pub trait FlvService {
    type Request;
    type Context;

    /// respond to request
    async fn respond(
        self: Arc<Self>,
        context: Self::Context,
        socket: FluvioSocket,
        connection: ConnectInfo
    ) -> Result<(), SocketError>;
}

pub struct InnerFlvApiServer<R, A, C, S, T> {
    req: PhantomData<R>,
    api: PhantomData<A>,
    context: C,
    service: Arc<S>,
    addr: String,
    builder: T,
}

impl <R, A, C, S, T> fmt::Debug for InnerFlvApiServer<R, A, C, S, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ApiServer({})", self.addr)
    }
}

impl<R, A, C, S, T> InnerFlvApiServer<R, A, C, S, T>
where
    C: Clone,
{
    pub fn inner_new(addr: String, context: C, service: S, builder: T) -> Self {
        InnerFlvApiServer {
            req: PhantomData,
            api: PhantomData,
            service: Arc::new(service),
            context,
            addr,
            builder,
        }
    }
}

pub type FlvApiServer<R, A, C, S> = InnerFlvApiServer<R, A, C, S, DefaultSocketBuilder>;

impl<R, A, C, S> FlvApiServer<R, A, C, S>
where
    C: Clone,
{
    pub fn new(addr: String, context: C, service: S) -> Self {
        Self::inner_new(addr, context, service, DefaultSocketBuilder {})
    }
}

impl<R, A, C, S, T> InnerFlvApiServer<R, A, C, S, T>
where
    R: ApiMessage<ApiKey = A> + Send + Debug + 'static,
    C: Clone + Sync + Send + Debug + 'static,
    A: Send + FluvioDecoder + Debug + 'static,
    S: FlvService<Request = R, Context = C> + Send + Sync + Debug + 'static,
    T: SocketBuilder + Send + Debug + 'static,
{
    pub fn run(self) -> Arc<SimpleEvent> {
        let event = SimpleEvent::shared();
        spawn(self.run_shutdown(event.clone()));

        event
    }

    async fn run_shutdown(self, shutdown_signal: Arc<SimpleEvent>) {
        match TcpListener::bind(&self.addr).await {
            Ok(listener) => {
                debug!(addr = %self.addr, "starting event loop");
                self.event_loop(listener, shutdown_signal).await;
            }
            Err(err) => {
                error!("error in shutting down: {}", err);
                process::exit(-1);
            }
        }
    }

    #[instrument(skip(listener, shutdown))]
    async fn event_loop(self, listener: TcpListener, shutdown: Arc<SimpleEvent>) {
        use tokio::select;

        let mut incoming = listener.incoming();
        debug!("opened connection listener");

        loop {
            debug!("waiting for client connection");

            select! {
                incoming = incoming.next() => {
                     self.serve_incoming(incoming)
                },
                _ = shutdown.listen()  => {
                    debug!("shutdown signal received");
                    break;
                }

            }
        }

        debug!("server terminating");
    }

    /// process incoming request, for each request, we create async task for serving
    #[instrument(skip(incoming))]
    fn serve_incoming(&self, incoming: Option<Result<TcpStream, IoError>>) {
        if let Some(incoming_stream) = incoming {
            match incoming_stream {
                Ok(stream) => {
                    let context = self.context.clone();
                    let service = self.service.clone();
                    let builder = self.builder.clone();
                    let host = self.addr.clone();

                    let ft = async move {
                        let address = stream
                            .peer_addr()
                            .map(|addr| addr.to_string())
                            .unwrap_or_else(|_| "".to_owned());

                        let peer = address.to_string();
                            
                        info!(server = %host,%peer, "new peer connection");


                        let socket_res = builder.to_socket(stream);
                        match socket_res.await {
                            Ok(socket) => {
                                let connection_info = ConnectInfo {
                                    peer: peer.clone(),
                                    host: host.clone()
                                };

                                if let Err(err) = service.respond(context.clone(), socket,connection_info).await {
                                    error!(
                                        "error handling stream: {}, shutdown on: {} from: {}",
                                        err, host,address, 
                                    );
                                } else {
                                    info!(%host,%peer, "connection terminated");
                                }
                            }
                            Err(err) => {
                                error!("error on tls handshake: {}, on: {} from: addr: {}", err, host, peer);
                            }
                        }
                    };

                    spawn(ft);
                }
                Err(err) => {
                    error!("error with stream: {}", err);
                }
            }
        } else {
            trace!("no stream value, ignoring");
        }
    }
}



#[cfg(test)]
mod test {

    use std::sync::Arc;
    use std::time::Duration;

    use tracing::debug;
    use tracing::trace;

    use fluvio_future::test_async;
    use fluvio_future::timer::sleep;

    use fluvio_protocol::api::RequestMessage;
    use fluvio_socket::FluvioSocket;
    use fluvio_socket::SocketError;

    use crate::test_request::EchoRequest;
    use crate::test_request::SharedTestContext;
    use crate::test_request::TestApiRequest;
    use crate::test_request::TestContext;
    use crate::test_request::TestKafkaApiEnum;
    use crate::test_request::TestService;

    use super::*;

    fn create_server(
        addr: String,
    ) -> FlvApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> {
        let ctx = Arc::new(TestContext::new());
        let server: FlvApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> =
            FlvApiServer::new(addr, ctx, TestService::new());

        server
    }

    async fn create_client(addr: String) -> Result<FluvioSocket, SocketError> {
        debug!("client wait for 1 second for 2nd server to come up");
        sleep(Duration::from_millis(100)).await;
        FluvioSocket::connect(&addr).await
    }

    async fn test_client(addr: String, shutdown: Arc<SimpleEvent>) {
        let mut socket = create_client(addr).await.expect("client");

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

        shutdown.notify();
    }

    #[test_async]
    async fn test_server() -> Result<(), SocketError> {
        // create fake server, anything will do since we only
        // care about creating tcp stream

        let socket_addr = "127.0.0.1:30001".to_owned();

        let server = create_server(socket_addr.clone());
        let shutdown = server.run();

        test_client(socket_addr.clone(), shutdown).await;

        Ok(())
    }
}
