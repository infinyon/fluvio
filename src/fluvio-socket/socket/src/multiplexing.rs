use core::task::{Context, Poll};
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_channel::bounded;
use async_channel::Receiver;
use async_channel::Sender;
use async_mutex::Mutex;
use bytes::BytesMut;
use event_listener::Event;
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::stream::{Stream, StreamExt};
use pin_project_lite::pin_project;
use tokio::select;
use tracing::{debug, error, instrument, trace};

use fluvio_future::net::TcpStream;
use fluvio_future::timer::sleep;
use fluvio_future::tls::AllTcpStream;
use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::Decoder;

use crate::FlvSocketError;
use crate::InnerExclusiveFlvSink;
use crate::InnerFlvSocket;
use crate::InnerFlvStream;

#[allow(unused)]
pub type DefaultMultiplexerSocket = MultiplexerSocket<TcpStream>;
pub type AllMultiplexerSocket = MultiplexerSocket<AllTcpStream>;

type SharedMsg = (Arc<Mutex<Option<BytesMut>>>, Arc<Event>);

/// Handle different way to multiplex
enum SharedSender {
    /// Serial socket
    Serial(SharedMsg),
    /// Batch Socket
    Queue(Sender<BytesMut>),
}

type Senders = Arc<Mutex<HashMap<i32, SharedSender>>>;

/// Socket that can multiplex connections
#[derive(Clone)]
pub struct MultiplexerSocket<S> {
    correlation_id_counter: Arc<Mutex<i32>>,
    senders: Senders,
    sink: InnerExclusiveFlvSink<S>,
}

impl<S> MultiplexerSocket<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    /// create new multiplexer socket, this always starts with correlation id of 1
    /// correlation id of 0 means shared
    pub fn new(socket: InnerFlvSocket<S>) -> Self {
        let (sink, stream) = socket.split();

        let multiplexer = Self {
            correlation_id_counter: Arc::new(Mutex::new(1)),
            senders: Arc::new(Mutex::new(HashMap::new())),
            sink: InnerExclusiveFlvSink::new(sink),
        };

        MultiPlexingResponseDispatcher::run(stream, multiplexer.senders.clone());

        multiplexer
    }

    /// get next available correlation to use
    //  use lock to ensure update happens in orderly manner
    async fn next_correlation_id(&self) -> i32 {
        let mut guard = self.correlation_id_counter.lock().await;
        let current_value = *guard;
        // update to new
        *guard = current_value + 1;
        current_value
    }

    /// create socket to perform request and response
    pub async fn create_serial_socket(&self) -> SerialSocket<S> {
        let correlation_id = self.next_correlation_id().await;
        let bytes_lock: SharedMsg = (Arc::new(Mutex::new(None)), Arc::new(Event::new()));

        let mut senders = self.senders.lock().await;
        senders.insert(correlation_id, SharedSender::Serial(bytes_lock.clone()));
        debug!("serial socket created with: {}", correlation_id);
        SerialSocket {
            sink: self.sink.clone(),
            correlation_id,
            receiver: bytes_lock,
        }
    }

    /// create stream response
    pub async fn create_stream<R>(
        &self,
        mut req_msg: RequestMessage<R>,
        queue_len: usize,
    ) -> Result<AsyncResponse<R>, FlvSocketError>
    where
        R: Request,
    {
        let correlation_id = self.next_correlation_id().await;
        req_msg.header.set_correlation_id(correlation_id);
        let (sender, receiver) = bounded(queue_len);

        let mut senders = self.senders.lock().await;
        senders.insert(correlation_id, SharedSender::Queue(sender));

        debug!("send async request with: {}", correlation_id);
        self.sink.send_request(&req_msg).await?;

        Ok(AsyncResponse {
            receiver,
            header: req_msg.header,
            correlation_id,
            data: PhantomData,
        })
    }
}

pin_project! {
    /// Implement async socket where response are send back async manner
    /// they are queued using channel
    pub struct AsyncResponse<R> {
        #[pin]
        receiver: Receiver<BytesMut>,
        header: RequestHeader,
        correlation_id: i32,
        data: PhantomData<R>,
    }
}

impl<R: Request> Stream for AsyncResponse<R> {
    type Item = Result<R::Response, FlvSocketError>;

    #[instrument(
        skip(self, cx),
        fields(
            api_key = R::API_KEY,
            correlation_id = self.correlation_id,
        )
    )]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next = match this.receiver.poll_next(cx) {
            Poll::Pending => {
                trace!("Waiting for async response");
                return Poll::Pending;
            }
            Poll::Ready(next) => next,
        };

        let bytes = match next {
            Some(bytes) => bytes,
            None => {
                error!("No more responses, server has terminated connection");
                // TODO REVIEW: Should this return None or Some(Err(...))?
                return Poll::Ready(None);
            }
        };

        let mut cursor = Cursor::new(&bytes);
        let response = R::Response::decode_from(&mut cursor, this.header.api_version());

        let value = match response {
            Ok(value) => {
                trace!("Received response: {:#?}", &value);
                Some(Ok(value))
            }
            Err(e) => Some(Err(e.into())),
        };
        Poll::Ready(value)
    }
}

pub type AllSerialSocket = SerialSocket<AllTcpStream>;

/// socket that can send request and response one at time,
/// this can be only created from multiplex socket
pub struct SerialSocket<S> {
    correlation_id: i32,
    sink: InnerExclusiveFlvSink<S>,
    receiver: SharedMsg,
}

impl<S> SerialSocket<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn send_and_receive<R>(
        &mut self,
        mut req_msg: RequestMessage<R>,
    ) -> Result<R::Response, FlvSocketError>
    where
        R: Request,
    {
        // first try to lock, this should lock
        // if lock fails then somebody still trying to  writing which should not happen, in this cases, we bail
        // if lock ok, then we cleared the value
        match self.receiver.0.try_lock() {
            Some(mut guard) => {
                debug!(
                    "serial socket: clearing existing value, id: {}",
                    self.correlation_id
                );
                *guard = None;
                drop(guard);
            }
            None => {
                return Err(IoError::new(
                    ErrorKind::BrokenPipe,
                    "invalid socket, try creating new one",
                )
                .into())
            }
        }

        req_msg.header.set_correlation_id(self.correlation_id);

        debug!("serial: sending serial request id: {}", self.correlation_id);
        trace!("sending request: {:#?}", req_msg);
        self.sink.send_request(&req_msg).await?;
        debug!(
            "serial: finished and waiting for reply from dispatcher for: {}",
            self.correlation_id
        );
        select! {
            _ = sleep(Duration::from_secs(5)) => {
                debug!("serial socket: timeout happen, id: {}",self.correlation_id);
                Err(IoError::new(
                    ErrorKind::TimedOut,
                    format!("time out in send and request: {}",self.correlation_id),
                ).into())
            },

            _ = self.receiver.1.listen() => {

                match self.receiver.0.try_lock() {
                    Some(guard) => {
                        debug!("serial socket: clearing existing value, id: {}",self.correlation_id);

                        if let Some(response_bytes) =  &*guard {

                            let response = R::Response::decode_from(
                                &mut Cursor::new(&response_bytes),
                                req_msg.header.api_version(),
                            )?;
                            trace!("receive response: {:#?}", response);
                            Ok(response)
                        } else {
                            debug!("serial socket: value is empty, something bad happened");
                            Err(IoError::new(
                                ErrorKind::UnexpectedEof,
                                "connection is closed".to_string(),
                            ).into())
                        }

                    },
                    None => Err(IoError::new(
                        ErrorKind::BrokenPipe,
                        "locked failed, socket is in bad state"
                    ).into())
                }
            },
        }
    }
}

/// This decodes fluvio protocol based streams and multiplex into different slots
struct MultiPlexingResponseDispatcher {
    senders: Senders,
}

impl MultiPlexingResponseDispatcher {
    pub fn run<S>(stream: InnerFlvStream<S>, senders: Senders)
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static + Send + Sync,
    {
        use fluvio_future::task::spawn;

        let dispatcher = Self { senders };

        debug!("dispatcher: spawning dispatcher loop");
        spawn(dispatcher.dispatcher_loop(stream));
    }

    async fn dispatcher_loop<S>(mut self, mut stream: InnerFlvStream<S>)
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static + Send + Sync,
    {
        let frame_stream = stream.get_mut_tcp_stream();

        loop {
            debug!("dispatcher: waiting for next response from stream ");

            if let Some(request) = frame_stream.next().await {
                if let Ok(mut msg) = request {
                    let mut correlation_id: i32 = 0;
                    match correlation_id.decode(&mut msg, 0) {
                        Ok(_) => {
                            debug!("dispatcher: decoded correlation id: {}", correlation_id);

                            if let Err(err) = self.send(correlation_id, msg).await {
                                error!("error sending to socket, {}", err)
                            }
                        }
                        Err(err) => error!("error decoding response, {}", err),
                    }
                } else {
                    debug!("dispatcher: problem getting frame from stream. terminating");
                    break;
                }
            } else {
                debug!("dispatcher: inner stream has terminated ");
                break;
            }
        }
    }

    /// send message to correct receiver
    pub async fn send(&mut self, correlation_id: i32, msg: BytesMut) -> Result<(), FlvSocketError> {
        let mut senders = self.senders.lock().await;
        if let Some(sender) = senders.get_mut(&correlation_id) {
            match sender {
                SharedSender::Serial(serial_sender) => {
                    // try lock
                    match serial_sender.0.try_lock() {
                        Some(mut guard) => {
                            *guard = Some(msg);
                            trace!("send back msg with correlation: {}", correlation_id);
                            drop(guard); // unlock
                            serial_sender.1.notify(1);
                            Ok(())
                        }
                        None => Err(IoError::new(
                            ErrorKind::BrokenPipe,
                            format!(
                                "failed locking, abandoning sending to socket: {}",
                                correlation_id
                            ),
                        )
                        .into()),
                    }
                }
                SharedSender::Queue(queue_sender) => queue_sender.send(msg).await.map_err(|_| {
                    IoError::new(
                        ErrorKind::BrokenPipe,
                        format!("problem sending to queue socket: {}", correlation_id),
                    )
                    .into()
                }),
            }
        } else {
            Err(IoError::new(
                ErrorKind::BrokenPipe,
                format!(
                    "no socket receiver founded for {}, abandoning sending",
                    correlation_id
                ),
            )
            .into())
        }
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use futures_util::future::{join, join3};
    use futures_util::StreamExt;
    use tracing::debug;

    use fluvio_future::net::TcpListener;
    use fluvio_future::task::spawn;
    use fluvio_future::test_async;
    use fluvio_future::timer::sleep;
    use fluvio_protocol::api::RequestMessage;

    use super::MultiplexerSocket;
    use crate::test_request::*;
    use crate::ExclusiveFlvSink;
    use crate::FlvSocket;
    use crate::FlvSocketError;

    async fn test_server(addr: &str) {
        let listener = TcpListener::bind(addr).await.expect("binding");
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let socket: FlvSocket = incoming_stream.into();

        let (sink, mut stream) = socket.split();

        let shared_sink = ExclusiveFlvSink::new(sink);

        let mut api_stream = stream.api_stream::<TestApiRequest, TestKafkaApiEnum>();

        for i in 0..3u16 {
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
                            sleep(Duration::from_millis(500)).await;
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
                        sleep(Duration::from_millis(30)).await;
                        let resp = status_request.new_response(AsyncStatusResponse {
                            status: status_request.request.count * 2,
                        });
                        reply_sink
                            .send_response(&resp, 0)
                            .await
                            .expect("send succeed");
                        debug!("server: send back status first");
                        sleep(Duration::from_millis(100)).await;
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

    async fn test_client(addr: &str) {
        use std::time::SystemTime;

        sleep(Duration::from_millis(20)).await;
        debug!("client: trying to connect");
        let socket = FlvSocket::connect(&addr).await.expect("connect");
        debug!("client: connected to test server and waiting...");
        sleep(Duration::from_millis(20)).await;
        let multiplexer = MultiplexerSocket::new(socket);
        let mut slow = multiplexer.create_serial_socket().await;
        let mut fast = multiplexer.create_serial_socket().await;

        // create async status
        let async_status_request = RequestMessage::new_request(AsyncStatusRequest { count: 2 });
        let mut status_response = multiplexer
            .create_stream(async_status_request, 10)
            .await
            .expect("response");

        let (slow, fast, _) = join3(
            async move {
                debug!("trying to send slow");
                // this message was send first but since there is delay of 500ms, it will return slower than fast
                let request = RequestMessage::new_request(EchoRequest::new("slow".to_owned()));
                let response = slow.send_and_receive(request).await.expect("send success");
                debug!("received slow response");
                assert_eq!(response.msg, "slow");
                SystemTime::now()
            },
            async move {
                // this message will be send later than slow but since there is no delay, it should get earlier than first
                sleep(Duration::from_millis(20)).await;
                debug!("trying to send fast");
                let request = RequestMessage::new_request(EchoRequest::new("fast".to_owned()));
                let response = fast.send_and_receive(request).await.expect("send success");
                debug!("received fast response");
                assert_eq!(response.msg, "hello");
                SystemTime::now()
            },
            async move {
                sleep(Duration::from_millis(100)).await;
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
    }

    #[test_async]
    async fn test_multiplexing() -> Result<(), FlvSocketError> {
        debug!("start testing");
        let addr = "127.0.0.1:6000";

        let _r = join(test_client(addr), test_server(addr)).await;
        Ok(())
    }
}
