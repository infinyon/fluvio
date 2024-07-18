use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use tracing::{trace, instrument};
use futures_util::SinkExt;
use async_lock::Mutex;
use async_lock::MutexGuard;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use tokio_util::codec::FramedWrite;

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
    enable_zero_copy: bool,
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
            enable_zero_copy: true,
            inner: SinkFrame::new(sink.compat_write(), FluvioCodec::new()),
        }
    }

    /// don't use zero copy
    pub fn disable_zerocopy(&mut self) {
        self.enable_zero_copy = false;
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
    use std::os::fd::BorrowedFd;

    use bytes::BytesMut;
    use fluvio_future::task::spawn_blocking;
    use futures_util::AsyncWriteExt;
    use nix::sys::uio::pread;

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
                            if self.enable_zero_copy {
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
                            } else {
                                let offset = f_slice.position() as i64;

                                #[cfg(all(target_pointer_width = "32", target_env = "gnu"))]
                                let offset: i32 = offset.try_into().unwrap();

                                let in_fd = f_slice.fd();
                                trace!(
                                    in_fd,
                                    offset,
                                    len = f_slice.len(),
                                    "reading from file slice"
                                );
                                let (read_result, mut buf) = spawn_blocking(move || {
                                    let mut buf = BytesMut::with_capacity(f_slice.len() as usize);
                                    buf.resize(f_slice.len() as usize, 0);
                                    let fd = unsafe { BorrowedFd::borrow_raw(in_fd) };
                                    let read_size = pread(fd, &mut buf, offset).map_err(|err| {
                                        IoError::new(
                                            ErrorKind::Other,
                                            format!("pread failed: {err}"),
                                        )
                                    });
                                    (read_size, buf)
                                })
                                .await;

                                let read = read_result?;
                                buf.resize(read, 0);

                                trace!(read, in_fd, buf_len = buf.len(), "status from file slice");

                                // write to socket
                                self.get_mut_tcp_sink()
                                    .get_mut()
                                    .get_mut()
                                    .write_all(&buf)
                                    .await?;

                                total_bytes_written += read;
                            }
                        }
                    }
                }
            }

            trace!(total_bytes_written, "finish writing store values");
            Ok(total_bytes_written)
        }
    }

    #[cfg(test)]
    mod tests {

        use std::io::Cursor;
        use std::io::ErrorKind;
        use std::sync::Arc;
        use std::time::Duration;
        use std::io::Error as IoError;

        use bytes::Buf;
        use bytes::BufMut;
        use bytes::BytesMut;
        use futures_util::AsyncWriteExt;
        use futures_util::future::join;
        use futures_util::StreamExt;
        use tracing::debug;

        use fluvio_future::file_slice::AsyncFileSlice;
        use fluvio_future::net::TcpListener;
        use fluvio_protocol::Version;
        use fluvio_protocol::store::FileWrite;
        use fluvio_protocol::store::StoreValue;
        use fluvio_future::fs::util;
        use fluvio_future::fs::AsyncFileExtension;
        use fluvio_future::timer::sleep;
        use fluvio_protocol::{Decoder, Encoder};
        use fluvio_types::event::StickyEvent;

        use crate::FluvioSocket;
        use crate::SocketError;

        // slice that outputs to socket with len and slice
        #[derive(Debug, Default)]
        struct SliceWrapper(AsyncFileSlice);

        impl SliceWrapper {
            pub fn len(&self) -> usize {
                self.0.len() as usize
            }

            pub fn raw_slice(&self) -> AsyncFileSlice {
                self.0.clone()
            }
        }

        impl Encoder for SliceWrapper {
            fn write_size(&self, _version: Version) -> usize {
                self.len() + 4 // include header
            }

            fn encode<T>(&self, src: &mut T, version: Version) -> Result<(), IoError>
            where
                T: BufMut,
            {
                // can only encode zero length
                if self.len() == 0 {
                    let len: u32 = 0;
                    len.encode(src, version)
                } else {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        format!("len {} is not zeo", self.len()),
                    ))
                }
            }
        }

        impl Decoder for SliceWrapper {
            fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
            where
                T: Buf,
            {
                unimplemented!("file slice cannot be decoded in the ButMut")
            }
        }

        impl FileWrite for SliceWrapper {
            fn file_encode(
                &self,
                _dest: &mut BytesMut,
                data: &mut Vec<StoreValue>,
                _version: Version,
            ) -> Result<(), IoError> {
                // just push slice
                data.push(StoreValue::FileSlice(self.raw_slice()));
                Ok(())
            }
        }

        async fn test_server(
            addr: &str,
            end: Arc<StickyEvent>,
            disable_zc: bool,
        ) -> Result<(), SocketError> {
            let listener = TcpListener::bind(&addr).await.expect("bind");
            debug!("server is running");
            let mut incoming = listener.incoming();

            end.notify();
            let incoming_stream = incoming.next().await;
            debug!("server: got connection");
            let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
            let mut socket: FluvioSocket = incoming_stream.into();

            let raw_tcp_sink = socket.get_mut_sink().get_mut_tcp_sink();

            const TEXT_LEN: u16 = 5;

            // directly encode total buffer with is 4 + 2 + string
            let mut out = vec![];
            let len: i32 = TEXT_LEN as i32 + 2; // msg plus file
            len.encode(&mut out, 0).expect("encode"); // codec len
            out.put_u16(TEXT_LEN); // string message len

            raw_tcp_sink.get_mut().get_mut().write_all(&out).await?;

            // send out file
            debug!("server: sending out file contents");
            let data_file = util::open("tests/test.txt").await.expect("open file");
            let fslice = data_file.as_slice(0, None).await.expect("slice");
            assert_eq!(fslice.len(), 5);
            let wrapper = SliceWrapper(fslice);

            let (mut sink, _stream) = socket.split();
            // output file slice
            if disable_zc {
                sink.disable_zerocopy();
            }
            sink.encode_file_slices(&wrapper, 0).await.expect("encode");

            debug!("server: hanging on client to test");
            // just in case if we need to keep it on
            sleep(Duration::from_millis(500)).await;
            debug!("server: finish");
            Ok(())
        }

        async fn setup_client(addr: &str, end: Arc<StickyEvent>) -> Result<(), SocketError> {
            debug!("waiting for server to start");
            while !end.is_set() {
                end.listen().await;
            }
            debug!("client: trying to connect");
            let mut socket = FluvioSocket::connect(addr).await.expect("connect");
            debug!("client: connect to test server and waiting for server to send out");
            let stream = socket.get_mut_stream();
            debug!("client: waiting for bytes");
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
        async fn test_sink_zero_copy() {
            let port = portpicker::pick_unused_port().expect("No free ports left");
            let addr = format!("127.0.0.1:{port}");

            let send_event = StickyEvent::shared();
            let _r = join(
                setup_client(&addr, send_event.clone()),
                test_server(&addr, send_event, false),
            )
            .await;
        }

        #[fluvio_future::test]
        async fn test_sink_buffer_copy() {
            let port = portpicker::pick_unused_port().expect("No free ports left");
            let addr = format!("127.0.0.1:{port}");

            let send_event = StickyEvent::shared();
            let _r = join(
                setup_client(&addr, send_event.clone()),
                test_server(&addr, send_event, true),
            )
            .await;
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
