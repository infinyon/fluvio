use core::task::{Context, Poll};
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::AtomicI32;
use std::sync::Mutex;
use std::time::Duration;
use std::fmt;
use std::future::Future;

use async_channel::bounded;
use async_channel::Receiver;
use async_channel::Sender;
use bytes::Bytes;
use event_listener::Event;
use fluvio_future::net::ConnectionFd;
use futures_util::stream::{Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use tokio::select;
use tracing::{info, warn};
use tracing::{debug, error, trace, instrument};

use fluvio_future::timer::sleep;
use futures_util::ready;
use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::Decoder;

use crate::SocketError;
use crate::ExclusiveFlvSink;
use crate::FluvioSocket;
use crate::FluvioStream;

pub type SharedMultiplexerSocket = Arc<MultiplexerSocket>;

#[derive(Clone)]
struct SharedMsg(Arc<Mutex<Option<Bytes>>>, Arc<Event>);

/// Handle different way to multiplex
enum SharedSender {
    /// Serial socket
    Serial(SharedMsg),
    /// Batch Socket
    Queue(Sender<Option<Bytes>>),
}

type Senders = Arc<async_lock::Mutex<HashMap<i32, SharedSender>>>;

/// Socket that can multiplex connections
pub struct MultiplexerSocket {
    correlation_id_counter: AtomicI32,
    senders: Senders,
    sink: ExclusiveFlvSink,
    stale: Arc<AtomicBool>,
    terminate: Arc<Event>,
}

impl fmt::Debug for MultiplexerSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiplexerSocket {}", self.sink.id())
    }
}

impl Drop for MultiplexerSocket {
    fn drop(&mut self) {
        // notify dispatcher
        self.terminate.notify(usize::MAX);
    }
}

impl MultiplexerSocket {
    pub fn shared(socket: FluvioSocket) -> Arc<Self> {
        Arc::new(Self::new(socket))
    }

    /// create new multiplexer socket, this always starts with correlation id of 1
    /// correlation id of 0 means shared
    #[allow(clippy::clone_on_copy)]
    pub fn new(socket: FluvioSocket) -> Self {
        let id = socket.id().clone();
        debug!(socket = %id, "spawning dispatcher");

        let (sink, stream) = socket.split();
        let stale = Arc::new(AtomicBool::new(false));

        let multiplexer = Self {
            correlation_id_counter: AtomicI32::new(1),
            senders: Arc::new(async_lock::Mutex::new(HashMap::new())),
            sink: ExclusiveFlvSink::new(sink),
            terminate: Arc::new(Event::new()),
            stale: stale.clone(),
        };

        MultiPlexingResponseDispatcher::run(
            id,
            stream,
            multiplexer.senders.clone(),
            multiplexer.terminate.clone(),
            stale,
        );

        multiplexer
    }

    pub fn set_stale(&self) {
        self.stale.store(true, SeqCst);
    }

    pub fn is_stale(&self) -> bool {
        self.stale.load(SeqCst)
    }

    /// get next available correlation to use
    fn next_correlation_id(&self) -> i32 {
        self.correlation_id_counter.fetch_add(1, SeqCst)
    }

    /// create socket to perform request and response
    #[instrument(skip(req_msg))]
    pub async fn send_and_receive<R>(
        &self,
        mut req_msg: RequestMessage<R>,
    ) -> Result<R::Response, SocketError>
    where
        R: Request,
    {
        use once_cell::sync::Lazy;

        static MAX_WAIT_TIME: Lazy<u64> = Lazy::new(|| {
            use std::env;

            let var_value = env::var("FLV_SOCKET_WAIT").unwrap_or_default();
            let wait_time: u64 = var_value.parse().unwrap_or(60);
            wait_time
        });

        let correlation_id = self.next_correlation_id();
        let bytes_lock = SharedMsg(Arc::new(Mutex::new(None)), Arc::new(Event::new()));

        req_msg.header.set_correlation_id(correlation_id);

        trace!(correlation_id, "senders trying lock");
        let mut senders = self.senders.lock().await;
        senders.insert(correlation_id, SharedSender::Serial(bytes_lock.clone()));
        drop(senders);

        let SharedMsg(msg, msg_event) = bytes_lock;
        // make sure we set up listener, otherwise dispatcher may notify before
        let listener = msg_event.listen();

        debug!(api = R::API_KEY, correlation_id, "sending request");
        self.sink.send_request(&req_msg).await?;
        trace!(correlation_id, "waiting");

        select! {

            _ = sleep(Duration::from_secs(*MAX_WAIT_TIME)) => {

                trace!("serial socket for: {}  timeout happen, id: {}", R::API_KEY, correlation_id);
                // clean channel
                let mut senders = self.senders.lock().await;
                senders.remove(&correlation_id);
                drop(senders);
                self.set_stale();


                Err(IoError::new(
                    ErrorKind::TimedOut,
                    format!("Timed out: {} secs waiting for response. API_KEY={}, CorrelationId={}", *MAX_WAIT_TIME,R::API_KEY, correlation_id),
                ).into())
            },

            _ = listener => {

                // clean channel
                trace!(correlation_id,"msg event");
                let mut senders = self.senders.lock().await;
                senders.remove(&correlation_id);
                drop(senders);

                match msg.try_lock() {
                    Ok(guard) => {

                        if let Some(response_bytes) =  &*guard {

                            debug!(correlation_id, len = response_bytes.len(),"receive serial message");
                            let response = R::Response::decode_from(
                                &mut Cursor::new(&response_bytes),
                                req_msg.header.api_version(),
                            )?;
                            trace!("receive serial socket id: {}, response: {:#?}", correlation_id, response);
                            Ok(response)
                        } else {
                            debug!("serial socket: {}, id: {}, value is empty, something bad happened",R::API_KEY,correlation_id);
                            Err(IoError::new(
                                ErrorKind::UnexpectedEof,
                                "connection is closed".to_string(),
                            ).into())
                        }

                    },
                    Err(e) => Err(IoError::new(
                        ErrorKind::BrokenPipe,
                        format!("locked failed: {correlation_id}, serial socket is in bad state, err: {e}"),
                    ).into())
                }
            },
        }
    }
    /// send request and get response asynchronously
    #[instrument(skip(req_msg))]
    pub async fn send_async<R>(
        &self,
        req_msg: RequestMessage<R>,
    ) -> Result<AsyncResponse<R>, SocketError>
    where
        R: Request,
    {
        self.create_stream(req_msg, 1).await
    }

    /// create stream response
    #[instrument(skip(self,req_msg), fields(api = R::API_KEY))]
    pub async fn create_stream<R>(
        &self,
        mut req_msg: RequestMessage<R>,
        queue_len: usize,
    ) -> Result<AsyncResponse<R>, SocketError>
    where
        R: Request,
    {
        let correlation_id = self.next_correlation_id();

        req_msg.header.set_correlation_id(correlation_id);

        trace!(correlation_id,request = ?req_msg, "new correlation id");

        // set up new channel
        let (sender, receiver) = bounded(queue_len);
        let mut senders = self.senders.lock().await;

        // remove any closed channel, this is not optimal but should do trick for now

        senders.retain(|_, shared_sender| match shared_sender {
            SharedSender::Serial(_) => true,
            SharedSender::Queue(sender) => !sender.is_closed(),
        });

        senders.insert(correlation_id, SharedSender::Queue(sender));
        drop(senders);

        trace!(correlation_id, "created new channel");

        self.sink.send_request(&req_msg).await?;

        trace!(correlation_id, "request send");

        // it is possible that msg have received by dispatcher before channel is inserted into senders
        // but it is easier to clean up

        Ok(AsyncResponse {
            receiver,
            header: req_msg.header,
            correlation_id,
            data: PhantomData,
        })
    }
}

/// Implement async socket where response are send back async manner
/// they are queued using channel
#[pin_project(PinnedDrop)]
pub struct AsyncResponse<R> {
    #[pin]
    receiver: Receiver<Option<Bytes>>,
    header: RequestHeader,
    correlation_id: i32,
    data: PhantomData<R>,
}

#[pinned_drop]
impl<R> PinnedDrop for AsyncResponse<R> {
    fn drop(self: Pin<&mut Self>) {
        self.receiver.close();
        debug!("multiplexer stream: {} closed", self.correlation_id);
    }
}

impl<R: Request> Stream for AsyncResponse<R> {
    type Item = Result<R::Response, SocketError>;

    #[instrument(
        skip(self, cx),
        fields(
            api_key = R::API_KEY,
            correlation_id = self.correlation_id,
        )
    )]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next: Option<Option<_>> = match this.receiver.poll_next(cx) {
            Poll::Pending => {
                trace!("Waiting for async response");
                return Poll::Pending;
            }
            Poll::Ready(next) => next,
        };

        if let Some(bytes) = next {
            if let Some(msg) = bytes {
                use bytes::Buf;
                let response_len = msg.len();
                debug!(
                    response_len,
                    remaining = msg.remaining(),
                    version = this.header.api_version(),
                    "response len>>>"
                );

                let mut cursor = Cursor::new(msg);
                let response = R::Response::decode_from(&mut cursor, this.header.api_version());
                let value = match response {
                    Ok(value) => {
                        trace!("Received response bytes: {},  {:#?}", response_len, &value,);
                        Some(Ok(value))
                    }
                    Err(e) => Some(Err(e.into())),
                };
                Poll::Ready(value)
            } else {
                Poll::Ready(Some(Err(SocketError::SocketClosed)))
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl<R: Request> Future for AsyncResponse<R> {
    type Output = Result<R::Response, SocketError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.poll_next(cx)) {
            Some(next) => Poll::Ready(next),
            None => Poll::Ready(Err(SocketError::SocketClosed)),
        }
    }
}

/// This decodes fluvio protocol based streams and multiplex into different slots
struct MultiPlexingResponseDispatcher {
    id: ConnectionFd,
    senders: Senders,
    terminate: Arc<Event>,
    stale: Arc<AtomicBool>,
}

impl fmt::Debug for MultiPlexingResponseDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MultiplexDisp({})", self.id)
    }
}

impl MultiPlexingResponseDispatcher {
    pub fn run(
        id: ConnectionFd,
        stream: FluvioStream,
        senders: Senders,
        terminate: Arc<Event>,
        stale: Arc<AtomicBool>,
    ) {
        use fluvio_future::task::spawn;

        let dispatcher = Self {
            id,
            senders,
            terminate,
            stale,
        };

        spawn(dispatcher.dispatcher_loop(stream));
    }

    #[instrument(skip(stream))]
    async fn dispatcher_loop(mut self, mut stream: FluvioStream) {
        let frame_stream = stream.get_mut_tcp_stream();

        loop {
            trace!("waiting");

            select! {
                frame = frame_stream.next() => {
                    match frame {
                        Some(Ok(mut msg)) => {
                            let mut correlation_id: i32 = 0;
                            match correlation_id.decode(&mut msg, 0) {
                                Ok(_) => {
                                    use bytes::Buf;
                                    debug!(correlation_id,len = msg.len(), remaining = msg.remaining(), "received frame");

                                    if let Err(err) = self.send(correlation_id, msg.freeze()).await {
                                        error!("error sending to socket, {}", err)
                                    }
                                }
                                Err(err) => error!("error decoding response, {}", err),
                            }
                        },
                        Some(Err(err)) => {
                            warn!("problem getting frame from stream: {err}. terminating");
                            self.close().await;
                            break;
                        },
                        None => {
                            info!("inner stream has terminated ");
                            self.close().await;
                            break;
                        }
                    }
                },

                _ = self.terminate.listen() => {
                    // terminate all channels

                    let guard = self.senders.lock().await;
                    for sender in guard.values() {
                        match sender {
                            SharedSender::Serial(msg) => msg.close(),
                            SharedSender::Queue(stream_sender) => {
                                stream_sender.close();
                            }
                        }
                    }

                    info!("multiplexer terminated");
                    break;

                }
            }
        }
    }

    /// send message to correct receiver
    #[instrument(skip(self, msg),fields( msg = msg.len()))]
    async fn send(&mut self, correlation_id: i32, msg: Bytes) -> Result<(), SocketError> {
        let mut senders = self.senders.lock().await;
        if let Some(sender) = senders.get_mut(&correlation_id) {
            match sender {
                SharedSender::Serial(serial_sender) => {
                    trace!("found serial");
                    // this should always succeed since nobody should lock
                    match serial_sender.0.try_lock() {
                        Ok(mut guard) => {
                            *guard = Some(msg);
                            drop(guard); // unlock
                            serial_sender.1.notify(1);
                            trace!("found serial");
                            Ok(())
                        }
                        Err(e) => Err(IoError::new(
                            ErrorKind::BrokenPipe,
                            format!(
                                "failed locking, abandoning sending to socket: {correlation_id}, err: {e}"
                            ),
                        )
                        .into()),
                    }
                }
                SharedSender::Queue(queue_sender) => {
                    trace!("found stream");
                    // sender was dropped before response arrives
                    if queue_sender.is_closed() {
                        debug!(correlation_id, "attempt to send data to closed socket");
                        Ok(())
                    } else {
                        queue_sender.send(Some(msg)).await.map_err(|err| {
                            IoError::new(
                                ErrorKind::BrokenPipe,
                                format!(
                                    "problem sending to queue socket: {correlation_id}, err: {err}"
                                ),
                            )
                            .into()
                        })
                    }
                }
            }
        } else {
            // sender was dropped and unregistered before response arrives
            debug!(
                correlation_id,
                "no socket receiver found, abandoning sending",
            );
            Ok(())
        }
    }

    async fn close(&self) {
        self.stale.store(true, SeqCst);

        let guard = self.senders.lock().await;
        for sender in guard.values() {
            match sender {
                SharedSender::Serial(msg) => msg.close(),
                SharedSender::Queue(stream_sender) => {
                    let _ = stream_sender.send(None).await;
                }
            }
        }

        info!("multiplexer closed")
    }
}

impl SharedMsg {
    fn close(&self) {
        let mut guard = self.0.lock().unwrap();
        *guard = None;
        drop(guard);
        self.1.notify(1);
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;
    use std::io::ErrorKind;

    use async_trait::async_trait;
    use futures_util::future::{join, join3};
    use futures_util::io::{AsyncRead, AsyncWrite};
    use futures_util::StreamExt;
    use tracing::debug;

    use fluvio_future::net::TcpListener;
    use fluvio_future::net::TcpStream;
    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;
    use fluvio_protocol::api::RequestMessage;

    use super::MultiplexerSocket;
    use super::SocketError;
    use crate::test_request::*;
    use crate::ExclusiveFlvSink;
    use crate::FluvioSocket;

    #[allow(unused)]
    const CA_PATH: &str = "certs/certs/ca.crt";
    #[allow(unused)]
    const X509_SERVER: &str = "certs/certs/server.crt";
    #[allow(unused)]
    const X509_SERVER_KEY: &str = "certs/certs/server.key";
    #[allow(unused)]
    const X509_CLIENT: &str = "certs/certs/client.crt";
    #[allow(unused)]
    const X509_CLIENT_KEY: &str = "certs/certs/client.key";

    #[allow(unused)]
    const SLEEP_MS: u64 = 10;

    #[async_trait]
    trait AcceptorHandler {
        type Stream: AsyncRead + AsyncWrite + Unpin + Send;
        async fn accept(&mut self, stream: TcpStream) -> FluvioSocket;
    }

    #[derive(Clone)]
    struct TcpStreamHandler {}

    #[async_trait]
    impl AcceptorHandler for TcpStreamHandler {
        type Stream = TcpStream;

        async fn accept(&mut self, stream: TcpStream) -> FluvioSocket {
            stream.into()
        }
    }

    fn get_error_kind<T: std::fmt::Debug>(
        result: Result<T, SocketError>,
    ) -> Option<std::io::ErrorKind> {
        match result {
            Err(SocketError::Io { source, .. }) => Some(source.kind()),
            _ => None,
        }
    }

    async fn test_server<A: AcceptorHandler + 'static>(
        addr: &str,
        mut handler: A,
        nb_iter: usize,
        timeout: u64,
    ) {
        let listener = TcpListener::bind(addr).await.expect("binding");
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let socket: FluvioSocket = handler.accept(incoming_stream).await;

        let (sink, mut stream) = socket.split();

        let shared_sink = ExclusiveFlvSink::new(sink);

        let mut api_stream = stream.api_stream::<TestApiRequest, TestKafkaApiEnum>();

        for i in 0..nb_iter {
            debug!("server: waiting for next msg: {}", i);
            let msg = api_stream.next().await.expect("msg").expect("unwrap");
            debug!("server: msg received: {:#?}", msg);

            match msg {
                TestApiRequest::EchoRequest(echo_request) => {
                    let mut reply_sink = shared_sink.clone();
                    // depends on different request we delay
                    if echo_request.request().msg == "slow" {
                        debug!("server: received slow msg");
                        spawn(async move {
                            sleep(Duration::from_millis(SLEEP_MS * 50)).await;
                            sleep(Duration::from_secs(timeout)).await; //simulate more waiting time from server while receiving slow msg
                            let resp =
                                echo_request.new_response(EchoResponse::new("slow".to_owned()));
                            debug!("server send slow response");
                            reply_sink
                                .send_response(&resp, 0)
                                .await
                                .expect("send succeed");
                        });
                    } else {
                        debug!("server: received fast msg");
                        spawn(async move {
                            let resp =
                                echo_request.new_response(EchoResponse::new("hello".to_owned()));
                            debug!("server: send fast response");
                            reply_sink
                                .send_response(&resp, 0)
                                .await
                                .expect("send succeed");
                        });
                    }
                }
                TestApiRequest::AsyncStatusRequest(status_request) => {
                    debug!("server: received async status msg");
                    let mut reply_sink = shared_sink.clone();
                    spawn(async move {
                        sleep(Duration::from_millis(SLEEP_MS * 3)).await;
                        let resp = status_request.new_response(AsyncStatusResponse {
                            status: status_request.request.count * 2,
                        });
                        reply_sink
                            .send_response(&resp, 0)
                            .await
                            .expect("send succeed");
                        debug!("server: send back status first");
                        sleep(Duration::from_millis(SLEEP_MS * 10)).await;
                        let resp = status_request.new_response(AsyncStatusResponse {
                            status: status_request.request.count * 4,
                        });
                        reply_sink
                            .send_response(&resp, 0)
                            .await
                            .expect("send succeed");
                        debug!("server: send back status second");
                    });
                }
                _ => panic!("no echo request"),
            }
        }

        debug!("server: finish sending out"); // finish ok
    }

    #[async_trait]
    trait ConnectorHandler {
        type Stream: AsyncRead + AsyncWrite + Unpin + Send + Sync;
        async fn connect(&mut self, stream: TcpStream) -> FluvioSocket;
    }

    #[async_trait]
    impl ConnectorHandler for TcpStreamHandler {
        type Stream = TcpStream;

        async fn connect(&mut self, stream: TcpStream) -> FluvioSocket {
            stream.into()
        }
    }

    async fn test_client<C: ConnectorHandler + 'static>(addr: &str, mut handler: C) {
        use std::time::SystemTime;

        sleep(Duration::from_millis(SLEEP_MS * 2)).await;
        debug!("client: trying to connect");
        let tcp_stream = TcpStream::connect(&addr).await.expect("connection fail");
        let socket = handler.connect(tcp_stream).await;
        debug!("client: connected to test server and waiting...");
        sleep(Duration::from_millis(SLEEP_MS * 2)).await;
        let multiplexer = MultiplexerSocket::shared(socket);

        // create async status
        let async_status_request = RequestMessage::new_request(AsyncStatusRequest { count: 2 });
        let mut status_response = multiplexer
            .create_stream(async_status_request, 10)
            .await
            .expect("response");

        let multiplexor2 = multiplexer.clone();
        let multiplexor3 = multiplexer.clone();

        let (slow, fast, _) = join3(
            async move {
                debug!("trying to send slow");
                // this message was send first but since there is delay of 500ms, it will return slower than fast
                let request = RequestMessage::new_request(EchoRequest::new("slow".to_owned()));
                let response = multiplexer
                    .send_and_receive(request)
                    .await
                    .expect("send success");
                debug!("received slow response");
                assert_eq!(response.msg, "slow");
                SystemTime::now()
            },
            async move {
                // this message will be send later than slow but since there is no delay, it should get earlier than first
                sleep(Duration::from_millis(SLEEP_MS * 2)).await;
                debug!("trying to send fast");
                let request = RequestMessage::new_request(EchoRequest::new("fast".to_owned()));
                let response = multiplexor2
                    .send_and_receive(request)
                    .await
                    .expect("send success");
                debug!("received fast response");
                assert_eq!(response.msg, "hello");
                SystemTime::now()
            },
            async move {
                sleep(Duration::from_millis(SLEEP_MS * 10)).await;
                let response = status_response
                    .next()
                    .await
                    .expect("stream yields value")
                    .expect("async response");
                debug!("received async response");
                assert_eq!(response.status, 4); // multiply by 2
                let response = status_response
                    .next()
                    .await
                    .expect("stream yields value")
                    .expect("async response");
                debug!("received async response");
                assert_eq!(response.status, 8);
                SystemTime::now()
            },
        )
        .await;

        assert!(slow > fast);

        // create async echo response
        let echo_request = RequestMessage::new_request(EchoRequest {
            msg: "fast".to_string(),
        });
        let echo_async_response = multiplexor3
            .send_async(echo_request)
            .await
            .expect("async response future");
        let response = echo_async_response.await.expect("async response");
        assert_eq!(response.msg, "hello");
    }

    async fn test_client_closed_socket<C: ConnectorHandler + 'static>(addr: &str, mut handler: C) {
        use std::time::SystemTime;

        sleep(Duration::from_millis(SLEEP_MS * 2)).await;
        debug!("client: trying to connect");
        let tcp_stream = TcpStream::connect(&addr).await.expect("connection fail");
        let socket = handler.connect(tcp_stream).await;
        debug!("client: connected to test server and waiting...");
        sleep(Duration::from_millis(SLEEP_MS * 2)).await;
        let multiplexer: std::sync::Arc<MultiplexerSocket> = MultiplexerSocket::shared(socket);

        let multiplexor2 = multiplexer.clone();

        let (slow, fast) = join(
            async move {
                debug!("trying to send slow");
                // this message was send first but since there is delay of 500ms, it will return slower than fast
                let request = RequestMessage::new_request(EchoRequest::new("slow".to_owned()));
                let response = multiplexer.send_and_receive(request).await;
                assert!(response.is_err());

                let err_kind = get_error_kind(response).expect("Get right Error Kind");
                let expected = ErrorKind::UnexpectedEof;
                assert_eq!(expected, err_kind);
                debug!("client: socket was closed");

                SystemTime::now()
            },
            async move {
                // this message will be send later than slow but since there is no delay, it should get earlier than first
                sleep(Duration::from_millis(SLEEP_MS * 2)).await;
                debug!("trying to send fast");
                let request = RequestMessage::new_request(EchoRequest::new("fast".to_owned()));
                let response = multiplexor2
                    .send_and_receive(request)
                    .await
                    .expect("send success");
                debug!("received fast response");
                assert_eq!(response.msg, "hello");
                multiplexor2.terminate.notify(usize::MAX); //close multiplexor2
                SystemTime::now()
            },
        )
        .await;
        assert!(slow > fast);
    }

    async fn test_client_time_out<C: ConnectorHandler + 'static>(addr: &str, mut handler: C) {
        sleep(Duration::from_millis(SLEEP_MS * 2)).await;
        debug!("client: trying to connect");
        let tcp_stream = TcpStream::connect(&addr).await.expect("connection fail");
        let socket = handler.connect(tcp_stream).await;
        debug!("client: connected to test server and waiting...");
        sleep(Duration::from_millis(SLEEP_MS * 2)).await;
        let multiplexer: std::sync::Arc<MultiplexerSocket> = MultiplexerSocket::shared(socket);

        let expected: ErrorKind = ErrorKind::TimedOut;

        debug!("trying to send slow");

        let request = RequestMessage::new_request(EchoRequest::new("slow".to_owned()));
        let response = multiplexer.send_and_receive(request).await;
        assert!(response.is_err());

        let err_kind = get_error_kind(response).expect("Get right Error Kind");

        assert_eq!(expected, err_kind);
        debug!("client: socket was timeout");
    }

    #[fluvio_future::test(ignore)]
    async fn test_multiplexing() {
        debug!("start testing");
        let addr = "127.0.0.1:6000";

        let _r = join(
            test_client(addr, TcpStreamHandler {}),
            test_server(addr, TcpStreamHandler {}, 4, 0),
        )
        .await;
    }

    #[fluvio_future::test(ignore)]
    async fn test_multiplexing_close_socket() {
        debug!("start test_multiplexing_close_socket");
        let addr = "127.0.0.1:6000";

        let _r = join(
            test_client_closed_socket(addr, TcpStreamHandler {}),
            test_server(addr, TcpStreamHandler {}, 2, 0),
        )
        .await;
    }

    #[fluvio_future::test(ignore)]
    async fn test_multiplexing_time_out() {
        debug!("start test_multiplexing_timeout");
        let addr = "127.0.0.1:6000";

        let _r = join(
            test_client_time_out(addr, TcpStreamHandler {}),
            test_server(addr, TcpStreamHandler {}, 1, 60), //MAX_WAIT_TIME is 60 second
        )
        .await;
    }
    #[cfg(unix)]
    mod tls_test {
        use std::os::unix::io::AsRawFd;

        use fluvio_future::{
            native_tls::{
                AcceptorBuilder, CertBuilder, ConnectorBuilder, DefaultClientTlsStream,
                DefaultServerTlsStream, IdentityBuilder, PrivateKeyBuilder, TlsAcceptor,
                TlsConnector, X509PemBuilder,
            },
            net::SplitConnection,
        };

        use super::*;

        struct TlsAcceptorHandler(TlsAcceptor);

        impl TlsAcceptorHandler {
            fn new() -> Self {
                let acceptor = AcceptorBuilder::identity(
                    IdentityBuilder::from_x509(
                        X509PemBuilder::from_path(X509_SERVER).expect("read"),
                        PrivateKeyBuilder::from_path(X509_SERVER_KEY).expect("file"),
                    )
                    .expect("identity"),
                )
                .expect("identity:")
                .build()
                .expect("acceptor");
                Self(acceptor)
            }
        }

        #[async_trait]
        impl AcceptorHandler for TlsAcceptorHandler {
            type Stream = DefaultServerTlsStream;

            async fn accept(&mut self, stream: TcpStream) -> FluvioSocket {
                let fd = stream.as_raw_fd();
                let handshake = self.0.accept(stream);
                let tls_stream = handshake.await.expect("hand shake failed");
                let (write, read) = tls_stream.split_connection();
                FluvioSocket::from_stream(write, read, fd)
            }
        }

        struct TlsConnectorHandler(TlsConnector);

        impl TlsConnectorHandler {
            fn new() -> Self {
                let connector = ConnectorBuilder::identity(
                    IdentityBuilder::from_x509(
                        X509PemBuilder::from_path(X509_CLIENT).expect("read"),
                        PrivateKeyBuilder::from_path(X509_CLIENT_KEY).expect("read"),
                    )
                    .expect("509"),
                )
                .expect("connector")
                .danger_accept_invalid_hostnames()
                .no_cert_verification()
                .build();
                Self(connector)
            }
        }

        #[async_trait]
        impl ConnectorHandler for TlsConnectorHandler {
            type Stream = DefaultClientTlsStream;

            async fn connect(&mut self, stream: TcpStream) -> FluvioSocket {
                let fd = stream.as_raw_fd();
                let (write, read) = self
                    .0
                    .connect("localhost", stream)
                    .await
                    .expect("hand shakefailed")
                    .split_connection();

                FluvioSocket::from_stream(write, read, fd)
            }
        }

        #[fluvio_future::test(ignore)]
        async fn test_multiplexing_native_tls() {
            debug!("start testing");
            let addr = "127.0.0.1:6001";

            let _r = join(
                test_client(addr, TlsConnectorHandler::new()),
                test_server(addr, TlsAcceptorHandler::new(), 4, 0),
            )
            .await;
        }
    }
}
