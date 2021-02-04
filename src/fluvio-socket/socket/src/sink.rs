use std::fmt::Debug;
use std::sync::Arc;

use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;

use async_mutex::Mutex;
use async_mutex::MutexGuard;
use bytes::Bytes;
use tracing::debug;
use tracing::trace;

use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use tokio_util::compat::Compat;

use bytes::BytesMut;
use fluvio_future::zero_copy::ZeroCopyWrite;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::api::ResponseMessage;
use fluvio_protocol::codec::FluvioCodec;
use fluvio_protocol::store::FileWrite;
use fluvio_protocol::store::StoreValue;
use fluvio_protocol::Encoder as FlvEncoder;
use fluvio_protocol::Version;

use fluvio_future::net::TcpStream;
use tokio_util::codec::Framed;

use crate::FlvSocketError;

pub type FlvSink = InnerFlvSink<TcpStream>;
pub type ExclusiveFlvSink = InnerExclusiveFlvSink<TcpStream>;

type SplitFrame<S> = SplitSink<Framed<Compat<S>, FluvioCodec>, Bytes>;

#[derive(Debug)]
pub struct InnerFlvSink<S> {
    inner: SplitFrame<S>,
    fd: RawFd,
}

impl<S> InnerFlvSink<S> {
    pub fn new(inner: SplitFrame<S>, fd: RawFd) -> Self {
        InnerFlvSink { fd, inner }
    }

    pub fn get_mut_tcp_sink(&mut self) -> &mut SplitFrame<S> {
        &mut self.inner
    }

    pub fn id(&self) -> RawFd {
        self.fd
    }

    /// convert to shared sink
    #[allow(clippy::wrong_self_convention)]
    pub fn as_shared(self) -> InnerExclusiveFlvSink<S> {
        InnerExclusiveFlvSink::new(self)
    }
}

impl<S> InnerFlvSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// as client, send request to server
    pub async fn send_request<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<(), FlvSocketError>
    where
        RequestMessage<R>: FlvEncoder + Default + Debug,
    {
        trace!("sending one way request: {:#?}", &req_msg);
        (&mut self.inner).send(req_msg.as_bytes(0)?).await?;
        Ok(())
    }

    /// as server, send back response
    pub async fn send_response<P>(
        &mut self,
        resp_msg: &ResponseMessage<P>,
        version: Version,
    ) -> Result<(), FlvSocketError>
    where
        ResponseMessage<P>: FlvEncoder + Default + Debug,
    {
        trace!("sending response {:#?}", &resp_msg);
        (&mut self.inner).send(resp_msg.as_bytes(version)?).await?;
        Ok(())
    }
}

impl<S> InnerFlvSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    Self: ZeroCopyWrite,
{
    /// write
    pub async fn encode_file_slices<T>(
        &mut self,
        msg: &T,
        version: Version,
    ) -> Result<(), FlvSocketError>
    where
        T: FileWrite,
    {
        trace!("encoding file slices version: {}", version);
        let mut buf = BytesMut::with_capacity(1000);
        let mut data: Vec<StoreValue> = vec![];
        msg.file_encode(&mut buf, &mut data, version)?;
        trace!("encoded buffer len: {}", buf.len());
        // add remainder
        data.push(StoreValue::Bytes(buf.freeze()));
        self.write_store_values(data).await
    }

    /// write store values to socket
    async fn write_store_values(&mut self, values: Vec<StoreValue>) -> Result<(), FlvSocketError> {
        trace!("writing store values to socket values: {}", values.len());

        for value in values {
            match value {
                StoreValue::Bytes(bytes) => {
                    debug!("writing store bytes to socket len: {}", bytes.len());
                    self.get_mut_tcp_sink().send(bytes).await?;
                }
                StoreValue::FileSlice(f_slice) => {
                    if f_slice.len() == 0 {
                        debug!("empty slice, skipping");
                    } else {
                        debug!(
                            "writing file slice pos: {} len: {} to socket",
                            f_slice.position(),
                            f_slice.len()
                        );
                        self.zero_copy_write(&f_slice).await?;
                        trace!("finish writing file slice");
                    }
                }
            }
        }

        Ok(())
    }
}

impl<S> AsRawFd for InnerFlvSink<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

/// Multi-thread aware Sink.  Only allow sending request one a time.
pub struct InnerExclusiveFlvSink<S> {
    inner: Arc<Mutex<InnerFlvSink<S>>>,
    fd: RawFd,
}

impl<S> InnerExclusiveFlvSink<S> {
    pub fn new(sink: InnerFlvSink<S>) -> Self {
        let fd = sink.id();
        InnerExclusiveFlvSink {
            inner: Arc::new(Mutex::new(sink)),
            fd,
        }
    }
}

impl<S> InnerExclusiveFlvSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn lock(&self) -> MutexGuard<'_, InnerFlvSink<S>> {
        self.inner.lock().await
    }

    pub async fn send_request<R>(&self, req_msg: &RequestMessage<R>) -> Result<(), FlvSocketError>
    where
        RequestMessage<R>: FlvEncoder + Default + Debug,
    {
        let mut inner_sink = self.inner.lock().await;
        inner_sink.send_request(req_msg).await
    }

    /// helper method to send back response
    pub async fn send_response<P>(
        &mut self,
        resp_msg: &ResponseMessage<P>,
        version: Version,
    ) -> Result<(), FlvSocketError>
    where
        ResponseMessage<P>: FlvEncoder + Default + Debug,
    {
        let mut inner_sink = self.inner.lock().await;
        inner_sink.send_response(resp_msg, version).await
    }

    pub fn id(&self) -> RawFd {
        self.fd
    }
}

impl<S> Clone for InnerExclusiveFlvSink<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fd: self.fd,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;
    use std::time::Duration;

    use async_net::TcpListener;
    use bytes::BufMut;
    use bytes::Bytes;
    use futures_util::future::join;
    use futures_util::{SinkExt, StreamExt};
    use tracing::debug;
    use tracing::info;

    use crate::FlvSocket;
    use crate::FlvSocketError;
    use fluvio_future::fs::util;
    use fluvio_future::fs::AsyncFileExtension;
    use fluvio_future::test_async;
    use fluvio_future::timer::sleep;
    use fluvio_future::zero_copy::ZeroCopyWrite;
    use fluvio_protocol::{Decoder, Encoder};

    async fn test_server(addr: &str) -> Result<(), FlvSocketError> {
        let listener = TcpListener::bind(&addr).await.expect("bind");
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: FlvSocket = incoming_stream.into();
        let raw_tcp_sink = socket.get_mut_sink().get_mut_tcp_sink();

        const TEXT_LEN: u16 = 5;

        // encode text file length as string
        let mut out = vec![];
        let len: i32 = TEXT_LEN as i32 + 2; // msg plus file
        len.encode(&mut out, 0).expect("encode"); // codec len
        out.put_u16(TEXT_LEN as u16); // string message len

        raw_tcp_sink.send(Bytes::from(out)).await.expect("send");

        // send out file
        debug!("sending out file contents");
        let data_file = util::open("tests/test.txt").await.expect("open file");
        let fslice = data_file.as_slice(0, None).await.expect("slice");
        socket
            .get_mut_sink()
            .zero_copy_write(&fslice)
            .await
            .expect("zero copy");

        // just in case if we need to keep it on
        sleep(Duration::from_millis(200)).await;
        debug!("server: finish sending out");
        Ok(())
    }

    async fn setup_client(addr: &str) -> Result<(), FlvSocketError> {
        sleep(Duration::from_millis(50)).await;
        debug!("client: trying to connect");
        let mut socket = FlvSocket::connect(&addr).await.expect("connect");
        info!("client: connect to test server and waiting...");
        let stream = socket.get_mut_stream();
        let next_value = stream.get_mut_tcp_stream().next().await;
        debug!("client: got bytes");
        let bytes = next_value.expect("next").expect("bytes");
        assert_eq!(bytes.len(), 7);
        debug!("decoding values");
        let mut src = Cursor::new(&bytes);
        let mut msg1 = String::new();
        msg1.decode(&mut src, 0).expect("decode should work");
        assert_eq!(msg1, "hello");

        Ok(())
    }

    #[test_async]
    async fn test_sink_copy() -> Result<(), FlvSocketError> {
        let addr = "127.0.0.1:9999";

        let _r = join(setup_client(addr), test_server(addr)).await;
        Ok(())
    }
}
