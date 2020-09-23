use std::fmt::Debug;
use std::io::Error as IoError;
use std::marker::PhantomData;

use std::sync::Arc;
use std::process;

use std::os::unix::io::AsRawFd;

use futures_util::StreamExt;
use event_listener::Event;
use futures_util::io::AsyncRead;
use futures_util::io::AsyncWrite;

use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::debug;
use tracing::instrument;
use async_trait::async_trait;

use fluvio_future::net::TcpListener;
use fluvio_future::net::TcpStream;
use fluvio_future::zero_copy::ZeroCopyWrite;
use fluvio_future::task::spawn;
use fluvio_protocol::api::ApiMessage;
use fluvio_protocol::Decoder as FluvioDecoder;
use fluvio_socket::InnerKfSocket;
use fluvio_socket::InnerKfSink;
use fluvio_socket::KfSocket;
use fluvio_socket::KfSocketError;
use fluvio_types::print_cli_err;

#[async_trait]
pub trait SocketBuilder: Clone {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send;

    async fn to_socket(
        &self,
        raw_stream: TcpStream,
    ) -> Result<InnerKfSocket<Self::Stream>, IoError>
    where
        InnerKfSink<Self::Stream>: ZeroCopyWrite;
}

#[derive(Debug, Clone)]
pub struct DefaultSocketBuilder {}

#[async_trait]
impl SocketBuilder for DefaultSocketBuilder {
    type Stream = TcpStream;

    async fn to_socket(
        &self,
        raw_stream: TcpStream,
    ) -> Result<InnerKfSocket<Self::Stream>, IoError> {
        let fd = raw_stream.as_raw_fd();
        Ok(KfSocket::from_stream(raw_stream, fd))
    }
}

/// Trait for responding to kf service
/// Request -> Response is type specific
/// Each response is responsible for sending back to socket
#[async_trait]
pub trait KfService<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    type Request;
    type Context;

    /// respond to request
    async fn respond(
        self: Arc<Self>,
        context: Self::Context,
        socket: InnerKfSocket<S>,
    ) -> Result<(), KfSocketError>
    where
        InnerKfSink<S>: ZeroCopyWrite;
}

/// Transform Service into Futures 01
#[derive(Debug)]
pub struct InnerKfApiServer<R, A, C, S, T> {
    req: PhantomData<R>,
    api: PhantomData<A>,
    context: C,
    service: Arc<S>,
    addr: String,
    builder: T,
}

impl<R, A, C, S, T> InnerKfApiServer<R, A, C, S, T>
where
    C: Clone,
{
    pub fn inner_new(addr: String, context: C, service: S, builder: T) -> Self {
        InnerKfApiServer {
            req: PhantomData,
            api: PhantomData,
            service: Arc::new(service),
            context,
            addr,
            builder,
        }
    }
}

pub type KfApiServer<R, A, C, S> = InnerKfApiServer<R, A, C, S, DefaultSocketBuilder>;

impl<R, A, C, S> KfApiServer<R, A, C, S>
where
    C: Clone,
{
    pub fn new(addr: String, context: C, service: S) -> Self {
        Self::inner_new(addr, context, service, DefaultSocketBuilder {})
    }
}

impl<R, A, C, S, T> InnerKfApiServer<R, A, C, S, T>
where
    R: ApiMessage<ApiKey = A> + Send + Debug + 'static,
    C: Clone + Sync + Send + Debug + 'static,
    A: Send + FluvioDecoder + Debug + 'static,
    S: KfService<T::Stream, Request = R, Context = C> + Send + Sync + Debug + 'static,
    T: SocketBuilder + Send + Debug + 'static,
    T::Stream: AsyncRead + AsyncWrite + Unpin + Send,
    InnerKfSink<T::Stream>: ZeroCopyWrite,
{
    pub fn run(self) -> Arc<Event> {
        let event = Arc::new(Event::new());

        spawn(self.run_shutdown(event.clone()));

        event
    }

    async fn run_shutdown(self, shutdown_signal: Arc<Event>) {
        match TcpListener::bind(&self.addr).await {
            Ok(listener) => {
                info!("starting event loop");
                self.event_loop(listener, shutdown_signal).await;
            }
            Err(err) => {
                print_cli_err!(err);
                process::exit(-1);
            }
        }
    }

    #[instrument(skip(self, listener, shutdown), fields(address = &*self.addr))]
    async fn event_loop(self, listener: TcpListener, shutdown: Arc<Event>) {
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
    #[instrument(skip(self, incoming))]
    fn serve_incoming(&self, incoming: Option<Result<TcpStream, IoError>>) {
        if let Some(incoming_stream) = incoming {
            match incoming_stream {
                Ok(stream) => {
                    let context = self.context.clone();
                    let service = self.service.clone();
                    let builder = self.builder.clone();

                    let ft = async move {
                        let address = stream
                            .peer_addr()
                            .map(|addr| addr.to_string())
                            .unwrap_or_else(|_| "".to_owned());
                        debug!(peer = &*address, "new peer connection");

                        let socket_res = builder.to_socket(stream);
                        match socket_res.await {
                            Ok(socket) => {
                                if let Err(err) = service.respond(context.clone(), socket).await {
                                    error!("error handling stream: {}", err);
                                }
                            }
                            Err(err) => {
                                error!("error on tls handshake: {}", err);
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

    use fluvio_future::timer::sleep;
    use fluvio_future::test_async;

    use fluvio_protocol::api::RequestMessage;
    use fluvio_socket::KfSocket;
    use fluvio_socket::KfSocketError;

    use crate::test_request::EchoRequest;
    use crate::test_request::SharedTestContext;
    use crate::test_request::TestApiRequest;
    use crate::test_request::TestContext;
    use crate::test_request::TestKafkaApiEnum;
    use crate::test_request::TestService;

    use super::*;

    fn create_server(
        addr: String,
    ) -> KfApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> {
        let ctx = Arc::new(TestContext::new());
        let server: KfApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> =
            KfApiServer::new(addr, ctx, TestService::new());

        server
    }

    async fn create_client(addr: String) -> Result<KfSocket, KfSocketError> {
        debug!("client wait for 1 second for 2nd server to come up");
        sleep(Duration::from_millis(100)).await;
        KfSocket::connect(&addr).await
    }

    async fn test_client(addr: String, shutdown: Arc<Event>) {
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

        shutdown.notify(1);
    }

    #[test_async]
    async fn test_server() -> Result<(), KfSocketError> {
        // create fake server, anything will do since we only
        // care about creating tcp stream

        let socket_addr = "127.0.0.1:30001".to_owned();

        let server = create_server(socket_addr.clone());
        let shutdown = server.run();

        test_client(socket_addr.clone(), shutdown).await;

        Ok(())
    }
}
