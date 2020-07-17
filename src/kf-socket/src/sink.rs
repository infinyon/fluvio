use std::fmt::Debug;

#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::os::unix::io::AsRawFd;

use log::trace;
use log::debug;
use bytes::Bytes;

use futures::sink::SinkExt;
use futures::stream::SplitSink;
use futures::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::Compat;

use flv_future_aio::zero_copy::ZeroCopyWrite;
use flv_future_aio::bytes::BytesMut;
use kf_protocol::Version;
use kf_protocol::Encoder as KfEncoder;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::transport::KfCodec;
use kf_protocol::fs::FileWrite;
use kf_protocol::fs::StoreValue;
use tokio_util::codec::Framed;
use flv_future_aio::net::TcpStream;
use flv_future_aio::net::tls::AllTcpStream;

use crate::KfSocketError;

pub type KfSink = InnerKfSink<TcpStream>;
#[allow(unused)]
pub type AllKfSink = InnerKfSink<AllTcpStream>;
pub type ExclusiveKfSink = InnerExclusiveKfSink<TcpStream>;
pub type ExclusiveAllKfSink = InnerExclusiveKfSink<AllTcpStream>;

type SplitFrame<S> = SplitSink<Framed<Compat<S>, KfCodec>, Bytes>;

#[derive(Debug)]
pub struct InnerKfSink<S> {
    inner: SplitFrame<S>,
    fd: RawFd,
}

impl<S> InnerKfSink<S> {
    pub fn new(inner: SplitFrame<S>, fd: RawFd) -> Self {
        InnerKfSink { fd, inner }
    }

    pub fn get_mut_tcp_sink(&mut self) -> &mut SplitFrame<S> {
        &mut self.inner
    }

    pub fn id(&self) -> RawFd {
        self.fd
    }

    /// convert to shared sink
    pub fn as_shared(self) -> InnerExclusiveKfSink<S> {
        InnerExclusiveKfSink::new(self)
    }
}

impl<S> InnerKfSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    

    /// as client, send request to server
    pub async fn send_request<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<(), KfSocketError>
    where
        RequestMessage<R>: KfEncoder + Default + Debug,
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
    ) -> Result<(), KfSocketError>
    where
        ResponseMessage<P>: KfEncoder + Default + Debug,
    {
        trace!("sending response {:#?}", &resp_msg);
        (&mut self.inner).send(resp_msg.as_bytes(version)?).await?;
        Ok(())
    }
}

impl<S> InnerKfSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    Self: ZeroCopyWrite,
{
    /// write
    pub async fn encode_file_slices<T>(
        &mut self,
        msg: &T,
        version: Version,
    ) -> Result<(), KfSocketError>
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
    async fn write_store_values(&mut self, values: Vec<StoreValue>) -> Result<(), KfSocketError> {
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

impl<S> AsRawFd for InnerKfSink<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

use async_lock::Lock;

/// Multi-thread aware Sink.  Only allow sending request one a time.
pub struct InnerExclusiveKfSink<S> {
    inner: Lock<InnerKfSink<S>>,
    fd: RawFd
}

impl<S> InnerExclusiveKfSink<S> {
    pub fn new(sink: InnerKfSink<S>) -> Self {
        let fd = sink.id();
        InnerExclusiveKfSink {
            inner: Lock::new(sink),
            fd
        }
    }
}

impl<S> InnerExclusiveKfSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    
    pub async fn send_request<R>(&self, req_msg: &RequestMessage<R>) -> Result<(), KfSocketError>
    where
        RequestMessage<R>: KfEncoder + Default + Debug,
    {
        let mut inner_sink = self.inner.lock().await;
        inner_sink.send_request(req_msg).await
    }

    pub async fn send_response<P>(
        &mut self,
        resp_msg: &ResponseMessage<P>,
        version: Version,
    ) -> Result<(), KfSocketError>
    where
        ResponseMessage<P>: KfEncoder + Default + Debug,
    {
        let mut inner_sink = self.inner.lock().await;
        inner_sink.send_response(resp_msg, version).await
    }


    pub fn id(&self) -> RawFd {
        self.fd
    }

    
}

impl<S> Clone for InnerExclusiveKfSink<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fd: self.fd.clone()
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;
    use std::path::Path;
    use std::time::Duration;
    use std::fs::remove_file;
    use std::env::temp_dir;

    use log::debug;
    use log::info;
    use futures::stream::StreamExt;
    use futures::future::join;
    use futures::io::AsyncWriteExt;
    use futures::sink::SinkExt;

    use flv_future_aio::test_async;
    use flv_future_aio::timer::sleep;
    use flv_future_aio::fs::util;
    use flv_future_aio::fs::AsyncFile;
    use flv_future_aio::zero_copy::ZeroCopyWrite;
    use flv_future_aio::net::TcpListener;
    use flv_future_aio::bytes::Bytes;
    use kf_protocol::Decoder;
    use kf_protocol::Encoder;
    use crate::KfSocket;
    use crate::KfSocketError;

    pub fn ensure_clean_file<P>(path: P)
    where
        P: AsRef<Path>,
    {
        let log_path = path.as_ref();
        if let Ok(_) = remove_file(log_path) {
            info!("remove existing file: {}", log_path.display());
        } else {
            info!("there was no existing file: {}", log_path.display());
        }
    }

    async fn test_server(addr: &str) -> Result<(), KfSocketError> {
        let listener = TcpListener::bind(&addr).await?;
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: KfSocket = incoming_stream.into();
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

    async fn setup_client(addr: &str) -> Result<(), KfSocketError> {
        sleep(Duration::from_millis(50)).await;
        debug!("client: trying to connect");
        let mut socket = KfSocket::connect(&addr).await?;
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
    async fn setup_data() -> Result<(), KfSocketError> {
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
    async fn test_sink_copy() -> Result<(), KfSocketError> {
        setup_data().await?;

        let addr = "127.0.0.1:9999";

        let _r = join(setup_client(addr), test_server(addr)).await;
        Ok(())
    }
}
