use std::io::Cursor;
use std::io::Error as IoError;

use log::trace;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use crate::core::Decoder as FluvioDecoder;
use crate::core::Encoder as FluvioEncoder;
use crate::core::bytes::{Bytes, BytesMut, BufMut};
use crate::core::Version;

/// Implement Kafka codec as in https://kafka.apache.org/protocol#The_Messages_ListOffsets
/// First 4 bytes are size of the message.  Then total buffer = 4 + message content
///
#[derive(Debug, Default)]
pub struct FluvioCodec {}

/// Type used as input by the [`FluvioCodec`] encoder implementation.
/// Contains the data of the message and the [`crate::core:Version`].
pub type FluvioCodecData<T> = (T, Version);

impl FluvioCodec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Decoder for FluvioCodec {
    type Item = BytesMut;
    type Error = IoError;

    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<BytesMut>, Self::Error> {
        let len = bytes.len();
        if len == 0 {
            return Ok(None);
        }
        if len >= 4 {
            let mut src = Cursor::new(&*bytes);
            let mut packet_len: i32 = 0;
            packet_len.decode(&mut src, 0)?;
            trace!(
                "Decoder: received buffer: {}, message size: {}",
                len,
                packet_len
            );
            if (packet_len + 4) as usize <= bytes.len() {
                trace!(
                    "Decoder: all packets are in buffer len: {}, excess {}",
                    packet_len + 4,
                    bytes.len() - (packet_len + 4) as usize
                );
                let mut buf = bytes.split_to((packet_len + 4) as usize);
                let message = buf.split_off(4); // truncate length
                Ok(Some(message))
            } else {
                trace!(
                    "Decoder buffer len: {} is less than packet+4: {}, waiting",
                    len,
                    packet_len + 4
                );
                Ok(None)
            }
        } else {
            trace!(
                "Decoder received raw bytes len: {} less than 4 not enough for size",
                len
            );
            Ok(None)
        }
    }
}

/// Implement encoder for Kafka Codec
/// This is straight pass thru, actual encoding is done file slice
impl Encoder<Bytes> for FluvioCodec {
    type Error = IoError;

    fn encode(&mut self, data: Bytes, buf: &mut BytesMut) -> Result<(), IoError> {
        trace!("Encoder: Encoding raw data with {} bytes", data.len());
        buf.put(data);
        Ok(())
    }
}

/// Implement encoder for Kafka Codec
impl<T: FluvioEncoder> Encoder<FluvioCodecData<T>> for FluvioCodec {
    type Error = IoError;

    fn encode(&mut self, src: FluvioCodecData<T>, buf: &mut BytesMut) -> Result<(), IoError> {
        let (src, version) = src;

        let size = src.write_size(version) as i32;
        trace!("encoding data with {} bytes.", size);
        buf.reserve(4 + size as usize);

        // First 4 bytes are the size of the message.
        // Then the message payload.
        let mut len_slice = Vec::new();
        size.encode(&mut len_slice, version)?;
        buf.extend_from_slice(&len_slice);
        buf.extend_from_slice(&src.as_bytes(version)?);

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::io::Error;
    use std::net::SocketAddr;
    use std::time;

    use futures::future::join;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use tokio_util::codec::Framed;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    use fluvio_future::net::TcpListener;
    use fluvio_future::net::TcpStream;
    use fluvio_future::timer::sleep;
    use fluvio_future::test_async;
    use futures::AsyncWriteExt;
    use fluvio_protocol::Decoder as FluvioDecoder;
    use fluvio_protocol::Encoder as FluvioEncoder;
    use log::debug;

    use super::FluvioCodec;

    async fn run_server_raw_data<T: FluvioEncoder>(
        data: T,
        addr: &SocketAddr,
    ) -> Result<(), Error> {
        debug!("server: binding");
        let listener = TcpListener::bind(&addr).await.expect("bind");
        debug!("server: successfully binding. waiting for incoming");
        let mut incoming = listener.incoming();
        if let Some(stream) = incoming.next().await {
            debug!("server: got connection from client");
            let mut tcp_stream = stream.expect("stream");

            // write message_size since we are not using the encoder
            let mut len_buf = vec![];
            let message_size = data.write_size(0) as i32;
            message_size.encode(&mut len_buf, 0).expect("encoding len");
            tcp_stream.write(&len_buf).await?;

            let encoded_data = data.as_bytes(0).expect("encoding data");
            tcp_stream.write(&encoded_data).await?;

            // Now trying partial send:
            // write message_size since we are not using the encoder
            let mut len_buf = vec![];
            let message_size = data.write_size(0) as i32;
            message_size.encode(&mut len_buf, 0).expect("encoding len");
            tcp_stream.write(&len_buf).await?;

            let mut encoded_data = data.as_bytes(0).expect("encoding data");
            let buf2 = encoded_data.split_off(3);
            tcp_stream.write(&encoded_data).await?;
            fluvio_future::timer::sleep(time::Duration::from_millis(10)).await;
            tcp_stream.write(&buf2).await?;
        }
        fluvio_future::timer::sleep(time::Duration::from_millis(50)).await;
        debug!("finishing. terminating server");
        Ok(())
    }

    async fn run_server_object<T: FluvioEncoder + Clone>(
        data: T,
        addr: &SocketAddr,
    ) -> Result<(), Error> {
        debug!("server: binding");
        let listener = TcpListener::bind(&addr).await.expect("bind");
        debug!("server: successfully binding. waiting for incoming");
        let mut incoming = listener.incoming();
        if let Some(stream) = incoming.next().await {
            debug!("server: got connection from client");
            let tcp_stream = stream.expect("stream");

            let framed = Framed::new(tcp_stream.compat(), FluvioCodec {});
            let (mut sink, _) = framed.split();

            // send 2 times in order
            for _ in 0..2_u8 {
                sink.send((data.clone(), 0)).await.expect("sending");
            }
        }
        fluvio_future::timer::sleep(time::Duration::from_millis(50)).await;
        debug!("finishing. terminating server");
        Ok(())
    }

    async fn run_client<
        T: PartialEq + std::fmt::Debug + Default + FluvioDecoder + FluvioEncoder,
    >(
        data: T,
        addr: &SocketAddr,
    ) -> Result<(), Error> {
        debug!("client: sleep to give server chance to come up");
        sleep(time::Duration::from_millis(100)).await;
        debug!("client: trying to connect");
        let tcp_stream = TcpStream::connect(&addr).await.expect("connect");
        debug!("client: got connection. waiting");
        let framed = Framed::new(tcp_stream.compat(), FluvioCodec {});
        let (_, mut stream) = framed.split::<(T, _)>();
        for _ in 0..2u16 {
            if let Some(value) = stream.next().await {
                debug!("client :received first value from server");
                let mut bytes = value.expect("bytes");
                let bytes_len = bytes.len();
                debug!("client: received bytes len: {}", bytes_len);
                let mut decoded_value = T::default();
                decoded_value
                    .decode(&mut bytes, 0)
                    .expect("decoding failed");
                assert_eq!(bytes_len, decoded_value.write_size(0));
                assert_eq!(decoded_value, data);
                debug!("all test pass");
            } else {
                panic!("no first value received");
            }
        }

        debug!("finished client");
        Ok(())
    }

    #[test_async]
    async fn test_async_tcp_vec() -> Result<(), Error> {
        debug!("start running test");

        let addr = "127.0.0.1:11223".parse::<SocketAddr>().expect("parse");
        let data: Vec<u8> = vec![0x1, 0x02, 0x03, 0x04, 0x5];

        let server_ft = run_server_object(data.clone(), &addr);
        let client_ft = run_client(data, &addr);

        let _rt = join(client_ft, server_ft).await;

        Ok(())
    }

    #[test_async]
    async fn test_async_tcp_string() -> Result<(), Error> {
        debug!("start running test");

        let addr = "127.0.0.1:11224".parse::<SocketAddr>().expect("parse");
        let data: String = String::from("hello");

        let server_ft = run_server_object(data.clone(), &addr);
        let client_ft = run_client(data, &addr);

        let _rt = join(client_ft, server_ft).await;

        Ok(())
    }

    #[allow(clippy::clone_on_copy)]
    #[test_async]
    async fn test_async_tcp_i32() -> Result<(), Error> {
        debug!("start running test");

        let addr = "127.0.0.1:11225".parse::<SocketAddr>().expect("parse");
        let data: i32 = 1000;

        let server_ft = run_server_object(data.clone(), &addr);
        let client_ft = run_client(data, &addr);

        let _rt = join(client_ft, server_ft).await;

        Ok(())
    }

    #[test_async]
    async fn test_async_tcp_raw_data() -> Result<(), Error> {
        debug!("start running test");

        let addr = "127.0.0.1:11226".parse::<SocketAddr>().expect("parse");
        let data: String = String::from("Raw text");

        let server_ft = run_server_raw_data(data.clone(), &addr);
        let client_ft = run_client(data, &addr);

        let _rt = join(client_ft, server_ft).await;

        Ok(())
    }
}
