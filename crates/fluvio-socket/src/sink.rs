use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use tracing::{trace, instrument};
use futures_util::{SinkExt};
use async_lock::Mutex;
use async_lock::MutexGuard;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use tokio_util::codec::{FramedWrite};

use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_protocol::codec::FluvioCodec;
use fluvio_protocol::Encoder as FlvEncoder;
use fluvio_protocol::Version;
use fluvio_future::net::{BoxWriteConnection, ConnectionFd};

use crate::SocketError;

type SinkFrame = FramedWrite<Compat<BoxWriteConnection>, FluvioCodec>;

pub struct FluvioSink {
    inner: SinkFrame,
    fd: ConnectionFd,
}

impl fmt::Debug for FluvioSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "fd({})", self.id())
    }
}

impl FluvioSink {
    pub fn get_mut_tcp_sink(&mut self) -> &mut SinkFrame {
        &mut self.inner
    }

    pub fn id(&self) -> ConnectionFd {
        #[allow(clippy::clone_on_copy)]
        self.fd.clone()
    }

    /// convert to shared sink
    #[allow(clippy::wrong_self_convention)]
    pub fn as_shared(self) -> ExclusiveFlvSink {
        ExclusiveFlvSink::new(self)
    }

    pub fn new(sink: BoxWriteConnection, fd: ConnectionFd) -> Self {
        Self {
            fd,
            inner: SinkFrame::new(sink.compat_write(), FluvioCodec::new()),
        }
    }

    /// as client, send request to server
    #[instrument(level = "trace",skip(req_msg),fields(req=?req_msg))]
    pub async fn send_request<R>(&mut self, req_msg: &RequestMessage<R>) -> Result<(), SocketError>
    where
        RequestMessage<R>: FlvEncoder + Debug,
    {
        self.inner.send((req_msg, 0)).await?;
        Ok(())
    }

    #[instrument(level = "trace", skip(resp_msg))]
    /// as server, send back response
    pub async fn send_response<P>(
        &mut self,
        resp_msg: &ResponseMessage<P>,
        version: Version,
    ) -> Result<(), SocketError>
    where
        ResponseMessage<P>: FlvEncoder + Debug,
    {
        trace!("sending response {:#?}", &resp_msg);
        self.inner.send((resp_msg, version)).await?;
        Ok(())
    }
}

#[cfg(unix)]
mod fd {

    use std::os::unix::io::AsRawFd;
    use std::os::unix::io::RawFd;

    use super::FluvioSink;

    impl AsRawFd for FluvioSink {
        fn as_raw_fd(&self) -> RawFd {
            self.fd
        }
    }
}

#[cfg(feature = "file")]
mod file {

    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use bytes::BytesMut;
    use futures_util::AsyncWriteExt;

    use fluvio_protocol::store::{FileWrite, StoreValue};
    use fluvio_future::zero_copy::ZeroCopy;

    use super::*;

    impl FluvioSink {
        /// write
        pub async fn encode_file_slices<T>(
            &mut self,
            msg: &T,
            version: Version,
        ) -> Result<usize, SocketError>
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
        async fn write_store_values(
            &mut self,
            values: Vec<StoreValue>,
        ) -> Result<usize, SocketError> {
            trace!("writing store values to socket values: {}", values.len());

            let mut total_bytes_written = 0usize;

            for value in values {
                match value {
                    StoreValue::Bytes(bytes) => {
                        trace!("writing store bytes to socket len: {}", bytes.len());
                        // These bytes should be already encoded so don't need to pass
                        // through the FluvioCodec
                        self.get_mut_tcp_sink()
                            .get_mut()
                            .get_mut()
                            .write_all(&bytes)
                            .await?;
                        total_bytes_written += bytes.len();
                    }
                    StoreValue::FileSlice(f_slice) => {
                        if f_slice.is_empty() {
                            trace!("empty slice, skipping");
                        } else {
                            trace!(
                                "writing file slice pos: {} len: {} to socket",
                                f_slice.position(),
                                f_slice.len()
                            );
                            let writer = ZeroCopy::raw(self.fd);
                            let bytes_written =
                                writer.copy_slice(&f_slice).await.map_err(|err| {
                                    IoError::new(
                                        ErrorKind::Other,
                                        format!("zero copy failed: {err}"),
                                    )
                                })?;
                            trace!("finish writing file slice with {bytes_written} bytes");
                            total_bytes_written += bytes_written;
                        }
                    }
                }
            }

            Ok(total_bytes_written)
        }
    }
}

/// Multi-thread aware Sink.  Only allow sending request one a time.
pub struct ExclusiveFlvSink {
    inner: Arc<Mutex<FluvioSink>>,
    fd: ConnectionFd,
}

impl ExclusiveFlvSink {
    pub fn new(sink: FluvioSink) -> Self {
        let fd = sink.id();
        ExclusiveFlvSink {
            inner: Arc::new(Mutex::new(sink)),
            fd,
        }
    }
}

impl ExclusiveFlvSink {
    pub async fn lock(&self) -> MutexGuard<'_, FluvioSink> {
        self.inner.lock().await
    }

    pub async fn send_request<R>(&self, req_msg: &RequestMessage<R>) -> Result<(), SocketError>
    where
        RequestMessage<R>: FlvEncoder + Debug,
    {
        let mut inner_sink = self.inner.lock().await;
        inner_sink.send_request(req_msg).await
    }

    /// helper method to send back response
    pub async fn send_response<P>(
        &mut self,
        resp_msg: &ResponseMessage<P>,
        version: Version,
    ) -> Result<(), SocketError>
    where
        ResponseMessage<P>: FlvEncoder + Debug,
    {
        let mut inner_sink = self.inner.lock().await;
        inner_sink.send_response(resp_msg, version).await
    }

    pub fn id(&self) -> ConnectionFd {
        #[allow(clippy::clone_on_copy)]
        self.fd.clone()
    }
}

impl Clone for ExclusiveFlvSink {
    fn clone(&self) -> Self {
        #[allow(clippy::clone_on_copy)]
        Self {
            inner: self.inner.clone(),
            fd: self.fd.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;
    use std::time::Duration;

    use async_net::TcpListener;
    use bytes::BufMut;
    use futures_util::future::join;
    use futures_util::StreamExt;
    use futures_util::io::AsyncWriteExt;
    use tracing::debug;
    use tracing::info;

    use crate::FluvioSocket;
    use crate::SocketError;
    use fluvio_future::fs::util;
    use fluvio_future::fs::AsyncFileExtension;
    use fluvio_future::timer::sleep;
    use fluvio_future::zero_copy::ZeroCopy;
    use fluvio_protocol::{Decoder, Encoder};

    async fn test_server(addr: &str) -> Result<(), SocketError> {
        let listener = TcpListener::bind(&addr).await.expect("bind");
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: FluvioSocket = incoming_stream.into();
        let raw_tcp_sink = socket.get_mut_sink().get_mut_tcp_sink();

        const TEXT_LEN: u16 = 5;

        // encode text file length as string
        let mut out = vec![];
        let len: i32 = TEXT_LEN as i32 + 2; // msg plus file
        len.encode(&mut out, 0).expect("encode"); // codec len
        out.put_u16(TEXT_LEN); // string message len

        raw_tcp_sink.get_mut().get_mut().write_all(&out).await?;

        // send out file
        debug!("sending out file contents");
        let data_file = util::open("tests/test.txt").await.expect("open file");
        let fslice = data_file.as_slice(0, None).await.expect("slice");

        let zerocopy = ZeroCopy::raw(socket.get_mut_sink().fd);
        zerocopy.copy_slice(&fslice).await.expect("zero copy");

        // just in case if we need to keep it on
        sleep(Duration::from_millis(200)).await;
        debug!("server: finish sending out");
        Ok(())
    }

    async fn setup_client(addr: &str) -> Result<(), SocketError> {
        sleep(Duration::from_millis(50)).await;
        debug!("client: trying to connect");
        let mut socket = FluvioSocket::connect(addr).await.expect("connect");
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

    #[fluvio_future::test]
    async fn test_sink_copy() {
        let port = portpicker::pick_unused_port().expect("No free ports left");
        let addr = format!("127.0.0.1:{port}");

        let _r = join(setup_client(&addr), test_server(&addr)).await;
    }
}
