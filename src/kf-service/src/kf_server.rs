use std::fmt::Debug;
use std::io::Error as IoError;
use std::marker::PhantomData;
use std::net::SocketAddr;

use std::sync::Arc;
use std::process;

use futures::StreamExt;
use futures::future::FutureExt;
use futures::select;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::channel;

use log::error;
use log::info;
use log::trace;
use log::debug;
use log::warn;
use async_trait::async_trait;

use flv_future_aio::net::AsyncTcpListener;
use flv_future_aio::net::AsyncTcpStream;
use flv_future_core::spawn;
use kf_protocol::api::KfRequestMessage;
use kf_protocol::Decoder as KfDecoder;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use types::print_cli_err;

/// Trait for responding to kf service
/// Request -> Response is type specific
/// Each response is responsible for sending back to socket
#[async_trait]
pub trait KfService {
    type Request;
    type Context;

    /// respond to request
    async fn respond(
        self: Arc<Self>,
        context: Self::Context,
        socket: KfSocket,
    ) -> Result<(), KfSocketError>;
}

/// Transform Service into Futures 01
pub struct KfApiServer<R, A, C, S> {
    req: PhantomData<R>,
    api: PhantomData<A>,
    context: C,
    service: Arc<S>,
    addr: SocketAddr,
}

impl<R, A, C, S> KfApiServer<R, A, C, S>
where
    C: Clone,
{
    pub fn new(addr: SocketAddr, context: C, service: S) -> Self {
        KfApiServer {
            req: PhantomData,
            api: PhantomData,
            service: Arc::new(service),
            context,
            addr,
        }
    }
}

impl<R, A, C, S> KfApiServer<R, A, C, S>
where
    R: KfRequestMessage<ApiKey = A> + Send + Debug + 'static,
    C: Clone + Sync + Send + 'static,
    A: Send + KfDecoder + Debug + 'static,
    S: KfService<Request = R, Context = C> + Send + 'static + Sync,
{
    pub fn run(self) -> Sender<bool> {
        let (sender, receiver) = channel::<bool>(1);

        spawn(self.run_shutdown(receiver));

        sender
    }

    pub async fn run_shutdown(self, shutdown_signal: Receiver<bool>) {
        match AsyncTcpListener::bind(&self.addr).await {
            Ok(listener) => {
                info!("starting event loop for: {}", &self.addr);
                self.event_loop(listener, shutdown_signal).await;
            }
            Err(err) => {
                print_cli_err!(err);
                process::exit(0x0100);
            }
        }
    }

    async fn event_loop(self, listener: AsyncTcpListener, mut shutdown_signal: Receiver<bool>) {
        let addr = self.addr;

        let mut incoming = listener.incoming();

        trace!("opened connection from: {}", addr);

        let mut done = false;

        while !done {
            trace!("waiting for client connection...");

            select! {
                incoming = incoming.next().fuse() => {
                     self.server_incoming(incoming)
                },
                shutdown = shutdown_signal.next()  => {
                    trace!("shutdown signal received");
                    if let Some(flag) = shutdown {
                        warn!("shutdown received");
                        done = true;
                    } else {
                        trace!("no shutdown value, ignoring");
                    }
                }

            }
        }

        info!("server terminating");
    }

    fn server_incoming(&self, incoming: Option<Result<AsyncTcpStream, IoError>>) {
        if let Some(incoming_stream) = incoming {
            match incoming_stream {
                Ok(stream) => {
                    let context = self.context.clone();
                    let service = self.service.clone();

                    let ft = async move {
                        debug!("new connection from {}", stream.peer_addr().map(|addr| addr.to_string()).unwrap_or("".to_owned()));
                        let socket: KfSocket = stream.into();

                        if let Err(err) = service.respond(context.clone(), socket).await {
                            error!("error handling stream: {}", err);
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

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::join;
    use futures::channel::mpsc::Sender;
    use futures::channel::mpsc::channel;
    use futures::sink::SinkExt;

    use log::debug;
    use log::trace;

    use flv_future_core::sleep;
    use flv_future_core::test_async;

    use kf_protocol::api::RequestMessage;
    use kf_socket::KfSocket;
    use kf_socket::KfSocketError;

    use crate::test_request::EchoRequest;
    use crate::test_request::SharedTestContext;
    use crate::test_request::TestApiRequest;
    use crate::test_request::TestContext;
    use crate::test_request::TestKafkaApiEnum;
    use crate::test_request::TestService;

    use super::KfApiServer;

    fn create_server(
        addr: SocketAddr,
    ) -> KfApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> {
        let ctx = Arc::new(TestContext::new());
        let server: KfApiServer<TestApiRequest, TestKafkaApiEnum, SharedTestContext, TestService> =
            KfApiServer::new(addr, ctx, TestService::new());

        server
    }

    async fn create_client(addr: SocketAddr) -> Result<KfSocket, KfSocketError> {
        debug!("client wait for 1 second for 2nd server to come up");
        sleep(Duration::from_millis(100)).await;
        KfSocket::connect(&addr).await
    }

    async fn test_client(
        addr: SocketAddr,
        mut shutdown: Sender<bool>,
    ) -> Result<(), KfSocketError> {
        let mut socket = create_client(addr).await?;

        let request = EchoRequest::new("hello".to_owned());
        let msg = RequestMessage::new_request(request);
        let reply = socket.send(&msg).await?;
        trace!("received reply from server: {:#?}", reply);
        assert_eq!(reply.response.msg, "hello");

        // send 2nd message on same socket
        let request2 = EchoRequest::new("hello2".to_owned());
        let msg2 = RequestMessage::new_request(request2);
        let reply2 = socket.send(&msg2).await?;
        trace!("received 2nd reply from server: {:#?}", reply2);
        assert_eq!(reply2.response.msg, "hello2");

        shutdown.send(true).await.expect("shutdown should succeed"); // shutdown server
        Ok(())
    }

    #[test_async]
    async fn test_server() -> Result<(), KfSocketError> {
        // create fake server, anything will do since we only
        // care about creating tcp stream

        let socket_addr = "127.0.0.1:30001".parse::<SocketAddr>().expect("parse");

        let (sender, receiver) = channel::<bool>(1);
        let server = create_server(socket_addr.clone());
        let client_ft1 = test_client(socket_addr.clone(), sender);

        let _r = join(client_ft1, server.run_shutdown(receiver)).await;

        Ok(())
    }
}
