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
use futures_util::sink::SinkExt;
use futures_util::stream::SplitSink;
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
use fluvio_future::tls::AllTcpStream;
use tokio_util::codec::Framed;

use crate::FlvSocketError;

pub type FlvSink = InnerFlvSink<TcpStream>;
#[allow(unused)]
pub type AllFlvSink = InnerFlvSink<AllTcpStream>;
pub type ExclusiveFlvSink = InnerExclusiveFlvSink<TcpStream>;
pub type ExclusiveAllFlvSink = InnerExclusiveFlvSink<AllTcpStream>;

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

    use std::env::temp_dir;
    use std::fs::remove_file;
    use std::io::Cursor;
    use std::path::Path;
    use std::time::Duration;

    use async_net::TcpListener;
    use bytes::Bytes;
    use futures_util::future::join;
    use futures_util::io::AsyncWriteExt;
    use futures_util::sink::SinkExt;
    use futures_util::stream::StreamExt;
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

    pub fn ensure_clean_file<P>(path: P)
    where
        P: AsRef<Path>,
    {
        let log_path = path.as_ref();
        if remove_file(log_path).is_ok() {
            info!("remove existing file: {}", log_path.display());
        } else {
            info!("there was no existing file: {}", log_path.display());
        }
    }

    async fn test_server(addr: &str) -> Result<(), FlvSocketError> {
        let listener = TcpListener::bind(&addr).await?;
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: FlvSocket = incoming_stream.into();
        let raw_tcp_sink = socket.get_mut_sink().get_mut_tcp_sink();

        // encode message
        let mut out = vec![];
        let msg = "hello".to_owned();
        msg.encode(&mut out, 0)?;

        // need to explicitly encode length since codec doesn't do anymore
        let mut buf = vec![];
        let len: i32 = out.len() as i32 + 7; // msg plus file
        len.encode(&mut buf, 0)?;
        msg.encode(&mut buf, 0)?;

        // send out raw bytes first
        debug!("out len: {}", buf.len());
        raw_tcp_sink.send(Bytes::from(buf)).await?;

        // send out file
        debug!("sending out file contents");
        let test_file_path = temp_dir().join("socket_zero_copy");
        let data_file = util::open(test_file_path).await?;
        let fslice = data_file.as_slice(0, None).await?;
        socket.get_mut_sink().zero_copy_write(&fslice).await?;

        debug!("server: finish sending out");
        Ok(())
    }

    async fn setup_client(addr: &str) -> Result<(), FlvSocketError> {
        sleep(Duration::from_millis(50)).await;
        debug!("client: trying to connect");
        let mut socket = FlvSocket::connect(&addr).await?;
        info!("client: connect to test server and waiting...");
        let stream = socket.get_mut_stream();
        let next_value = stream.get_mut_tcp_stream().next().await;
        let bytes = next_value.expect("next").expect("bytes");
        debug!("decoding values");
        let mut src = Cursor::new(&bytes);

        let mut msg1 = String::new();
        msg1.decode(&mut src, 0).expect("decode should work");
        assert_eq!(msg1, "hello");

        let mut msg2 = String::new();
        msg2.decode(&mut src, 0)
            .expect("2nd msg decoding should work");
        debug!("msg2: {}", msg2);
        assert_eq!(msg2, "world");
        Ok(())
    }
    // set up sample file for testing
    async fn setup_data() -> Result<(), FlvSocketError> {
        let test_file_path = temp_dir().join("socket_zero_copy");
        ensure_clean_file(&test_file_path);
        debug!("creating test file: {:#?}", test_file_path);
        let mut file = util::create(&test_file_path).await?;
        let mut out = vec![];
        let msg = "world".to_owned();
        msg.encode(&mut out, 0)?;
        file.write_all(&out).await?;
        Ok(())
    }

    #[test_async]
    async fn test_sink_copy() -> Result<(), FlvSocketError> {
        setup_data().await?;

        let addr = "127.0.0.1:9999";

        let _r = join(setup_client(addr), test_server(addr)).await;
        Ok(())
    }
}
