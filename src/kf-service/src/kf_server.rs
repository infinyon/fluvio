use std::fmt::Debug;
use std::io::Error as IoError;
use std::marker::PhantomData;


use std::sync::Arc;
use std::process;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use futures::StreamExt;
use futures::future::FutureExt;
use futures::select;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::channel;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;

use log::error;
use log::info;
use log::trace;
use log::debug;
use log::warn;
use async_trait::async_trait;

use flv_future_aio::net::TcpListener;
use flv_future_aio::net::TcpStream;
use flv_future_aio::zero_copy::ZeroCopyWrite;
use flv_future_aio::task::spawn;
use kf_protocol::api::KfRequestMessage;
use kf_protocol::Decoder as KfDecoder;
use kf_socket::InnerKfSocket;
use kf_socket::InnerKfSink;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use flv_types::print_cli_err;

#[async_trait]
pub trait SocketBuilder: Clone {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send;

    async fn to_socket(
        &self,
        raw_stream: TcpStream,
    ) -> Result<InnerKfSocket<Self::Stream>, IoError> where InnerKfSink<Self::Stream>: ZeroCopyWrite;
}


#[derive(Clone)]
pub struct DefaultSocketBuilder{}


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
pub trait KfService<S> where S:  AsyncRead + AsyncWrite + Unpin + Send{
    type Request;
    type Context;


    /// respond to request
    async fn respond(
        self: Arc<Self>,
        context: Self::Context,
        socket: InnerKfSocket<S>,
    ) -> Result<(), KfSocketError>
        where InnerKfSink<S>: ZeroCopyWrite;
}


/// Transform Service into Futures 01
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


pub type KfApiServer<R,A,C,S> = InnerKfApiServer<R,A,C,S,DefaultSocketBuilder>;


impl<R, A, C, S> KfApiServer<R, A, C, S>
where
    C: Clone,
{
    pub fn new(addr: String, context: C, service: S) -> Self {
        Self::inner_new(addr,context,service, DefaultSocketBuilder{})
    }
}



impl<R, A, C, S, T> InnerKfApiServer<R, A, C, S, T>
where
    R: KfRequestMessage<ApiKey = A> + Send + Debug + 'static,
    C: Clone + Sync + Send + 'static,
    A: Send + KfDecoder + Debug + 'static,
    S: KfService<T::Stream,Request = R, Context = C > + Send + 'static + Sync,
    T: SocketBuilder + Send + 'static,
    T::Stream: AsyncRead + AsyncWrite + Unpin + Send,
    InnerKfSink<T::Stream>: ZeroCopyWrite
{
    pub fn run(self) -> Sender<bool> {
        let (sender, receiver) = channel::<bool>(1);

        spawn(self.run_shutdown(receiver));

        sender
    }

    pub async fn run_shutdown(self, shutdown_signal: Receiver<bool>) {
        match TcpListener::bind(&self.addr).await {
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

    async fn event_loop(self, listener: TcpListener, mut shutdown_signal: Receiver<bool>) {
        
        let addr = self.addr.clone();

        let mut incoming = listener.incoming();

        debug!("listening connection on {}", addr);

        let mut done = false;

        while !done {
            debug!("waiting for client connection...");

            select! {
                incoming = incoming.next().fuse() => {
                     self.serve_incoming(incoming)
                },
                shutdown = shutdown_signal.next()  => {
                    debug!("shutdown signal received");
                    if let Some(flag) = shutdown {
                        warn!("shutdown received");
                        done = true;
                    } else {
                        debug!("no shutdown value, ignoring");
                    }
                }

            }
        }

        info!("server terminating");
    }

    /// process incoming request, for each request, we create async task for serving
    fn serve_incoming(&self, incoming: Option<Result<TcpStream, IoError>>) {
        if let Some(incoming_stream) = incoming {
            match incoming_stream {
                Ok(stream) => {
                    
                    let context = self.context.clone();
                    let service = self.service.clone();
                    let builder = self.builder.clone();

                    let ft = async move {

                        debug!(
                            "new connection from {}",
                            stream
                                .peer_addr()
                                .map(|addr| addr.to_string())
                                .unwrap_or("".to_owned())
                        );

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

    use futures::future::join;
    use futures::channel::mpsc::Sender;
    use futures::channel::mpsc::channel;
    use futures::sink::SinkExt;

    use log::debug;
    use log::trace;

    use flv_future_aio::timer::sleep;
    use flv_future_aio::test_async;

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

    async fn test_client(
        addr: String,
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

        let socket_addr = "127.0.0.1:30001".to_owned();

        let (sender, receiver) = channel::<bool>(1);
        let server = create_server(socket_addr.clone());
        let client_ft1 = test_client(socket_addr.clone(), sender);

        let _r = join(client_ft1, server.run_shutdown(receiver)).await;

        Ok(())
    }
}
