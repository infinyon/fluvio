use std::io::Cursor;
use std::io::Error as IoError;

use log::trace;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use crate::core::Decoder as FluvioDecoder;

/// Implement Kafka codec as in https://kafka.apache.org/protocol#The_Messages_ListOffsets
/// First 4 bytes are size of the message.  Then total buffer = 4 + message content
///
#[derive(Debug, Default)]
pub struct FluvioCodec {}

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
            return Ok(None)
        }
        if len >= 4 {
            let mut src = Cursor::new(&*bytes);
            let mut packet_len: i32 = 0;
            packet_len.decode(&mut src, 0)?;
            trace!("Decoder: received buffer: {}, message size: {}", len, packet_len);
            if (packet_len + 4) as usize <= bytes.len() {
                trace!(
                    "Decoder: all packets are in buffer len: {}, excess {}",
                    packet_len + 4,
                    bytes.len() - (packet_len + 4) as usize
                );
                let mut buf = bytes.split_to((packet_len + 4) as usize);
                let message = buf.split_off(4);   // truncate length
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
            trace!("Decoder received raw bytes len: {} less than 4 not enough for size", len);
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

#[cfg(test)]
mod test {

    use std::io::Error;
    use std::net::SocketAddr;
    use std::time;

    use bytes::Bytes;
    use futures::future::join;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use tokio_util::codec::Framed;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    use fluvio_future::net::TcpListener;
    use fluvio_future::net::TcpStream;
    use fluvio_future::timer::sleep;
    use fluvio_future::test_async;
    use fluvio_protocol::Decoder as FluvioDecoder;
    use fluvio_protocol::Encoder as FluvioEncoder;
    use log::debug;

    use super::FluvioCodec;

    #[test_async]
    async fn test_async_tcp() -> Result<(), Error> {
        debug!("start running test");

        let addr = "127.0.0.1:11122".parse::<SocketAddr>().expect("parse");

        let server_ft = async {
            debug!("server: binding");
            let listener = TcpListener::bind(&addr).await.expect("bind");
            debug!("server: successfully binding. waiting for incoming");
            let mut incoming = listener.incoming();
            while let Some(stream) = incoming.next().await {
                debug!("server: got connection from client");
                let tcp_stream = stream.expect("stream");

                let framed = Framed::new(tcp_stream.compat(), FluvioCodec {});
                let (mut sink, _) = framed.split();

                let data: Vec<u8> = vec![0x1, 0x02, 0x03, 0x04, 0x5];
                // send 2 times in order
                for _ in 0..2u16  {

                    //  debug!("server encoding original vector with len: {}", data.len());
                    let mut buf = vec![];
                    data.encode(&mut buf, 0)?;
                    debug!(
                        "server: writing to client vector encoded len: {}",
                        buf.len()
                    );
                    assert_eq!(buf.len(), 9); //  4(array len)+ 5 bytes

                    // write buffer length since encoder doesn't write
                    // need to send out len
                    let mut len_buf = vec![];
                    let len = buf.len() as i32;
                    len.encode(&mut len_buf, 0).expect("encoding");
                    sink.send(Bytes::from(len_buf)).await.expect("sending");

                    sink.send(Bytes::from(buf)).await.expect("sending");
                }

                // last one, we send split send, to test partial send
                {
                    let mut buf = vec![];
                    data.encode(&mut buf, 0)?;
                    debug!(
                        "server: writing to client vector encoded len: {}",
                        buf.len()
                    );
                    assert_eq!(buf.len(), 9); //  4(array len)+ 5 bytes

                    // write buffer length since encoder doesn't write
                    // need to send out len
                    let mut len_buf = vec![];
                    let len = buf.len() as i32;
                    len.encode(&mut len_buf, 0).expect("encoding");
                    sink.send(Bytes::from(len_buf)).await.expect("sending");

                    // split buf into two segments, decode should reassembly them
                    let buf2 = buf.split_off(5);
                    sink.send(Bytes::from(buf)).await.expect("sending");
                    fluvio_future::timer::sleep(time::Duration::from_millis(10)).await;
                    sink.send(Bytes::from(buf2)).await.expect("sending");

                }

                fluvio_future::timer::sleep(time::Duration::from_millis(50)).await;
                debug!("finishing. terminating server");
                return Ok(()) as Result<(), Error>;
            }

            Ok(()) as Result<(), Error>
        };

        let client_ft = async {
            debug!("client: sleep to give server chance to come up");
            sleep(time::Duration::from_millis(100)).await;
            debug!("client: trying to connect");
            let tcp_stream = TcpStream::connect(&addr).await.expect("connect");
            debug!("client: got connection. waiting");
            let framed = Framed::new(tcp_stream.compat(), FluvioCodec{});
            let (_, mut stream) = framed.split();
            for _ in 0..3u16 {
                if let Some(value) = stream.next().await {
                    debug!("client :received first value from server");
                    let mut bytes = value.expect("bytes");
                    debug!("client: received bytes len: {}", bytes.len());
                    assert_eq!(bytes.len(), 9, "total bytes is 6");
                    let mut decoded_values: Vec<u8> = vec![];
                    decoded_values
                        .decode(&mut bytes, 0)
                        .expect("vector decoding failed");
                    assert_eq!(decoded_values.len(), 5);
                    assert_eq!(decoded_values[0], 1);
                    assert_eq!(decoded_values[1], 2);
                    debug!("all test pass");
                } else {
                    assert!(false, "no first value received");
                }
            }


            debug!("finished client");

            Ok(()) as Result<(), Error>
        };

        let _rt = join(client_ft, server_ft).await;

        Ok(())
    }
}
