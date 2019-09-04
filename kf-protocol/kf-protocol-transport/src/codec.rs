use std::io::Cursor;
use std::io::Error as IoError;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use log::trace;
use tokio_codec::Decoder;
use tokio_codec::Encoder;

use kf_protocol::Decoder as KDecoder;

#[derive(Debug, Default)]
pub struct KfCodec(());

impl Decoder for KfCodec {
    type Item = BytesMut;
    type Error = IoError;

    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<BytesMut>, Self::Error> {
        let len = bytes.len();
        trace!("Codec decoder: received bytes from buf: {}", len);
        if len >= 4 {
            let mut src = Cursor::new(&*bytes);
            let mut packet_len: i32 = 0;
            packet_len.decode(&mut src,0)?;
            trace!("Codec decoder content len: {}", packet_len);
            if (packet_len + 4) as usize <= len {
                trace!(
                    "Codec decoder: fully decoded packet len+4: {} ",
                    packet_len + 4
                );
                bytes.advance(4);
                Ok(Some(bytes.split_to(packet_len as usize)))
            } else {
                trace!(
                    "Codec decoder buffer len: {} is less than packet+4: {}",
                    len,
                    packet_len + 4
                );
                Ok(None)
            }
        } else {
            trace!("Codec decoder not enough to decode len: {}", len);
            Ok(None)
        }
    }
}

impl Encoder for KfCodec {
    type Item = Bytes;
    type Error = IoError;

    fn encode(&mut self, data: Bytes, buf: &mut BytesMut) -> Result<(), IoError> {
        trace!("Codec encoder: writing {} bytes", data.len());
        buf.reserve(data.len());
        buf.put(data);
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;
    use std::io::Error;
    use std::net::SocketAddr;
    use std::time;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use futures::future::join;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;

    use future_aio::net::AsyncTcpListener;
    use future_aio::net::AsyncTcpStream;
    use future_aio::net::TcpStreamSplit;
    use future_helper::sleep;
    use future_helper::test_async;
    use kf_protocol::Decoder as KDecoder;
    use kf_protocol::Encoder as KEncoder;
    use log::debug;

    use super::KfCodec;

    fn to_bytes(bytes: Vec<u8>) -> Bytes {
        let mut buf = BytesMut::with_capacity(bytes.len());
        buf.put_slice(&bytes);
        buf.freeze()
    }

    #[test_async]
    async fn test_async_tcp() -> Result<(), Error> {
        debug!("start running test");

        let addr = "127.0.0.1:11122".parse::<SocketAddr>().expect("parse");

        let server_ft = async {
            debug!("server: binding");
            let listener = AsyncTcpListener::bind(&addr)?;
            debug!("server: successfully binding. waiting for incoming");
            let mut incoming = listener.incoming();
            while let Some(stream) = incoming.next().await {
                debug!("server: got connection from client");
                let tcp_stream = stream?;
                let split: TcpStreamSplit<KfCodec> = tcp_stream.split();
                let mut sink = split.sink();
                let data: Vec<u8> = vec![0x1, 0x02, 0x03, 0x04, 0x5];
                debug!("data len: {}", data.len());
                let mut buf = vec![];
                debug!("server buf len: {}", buf.len());
                data.encode(&mut buf,0)?;
                debug!("server buf len: {}", buf.len());
                let out = to_bytes(buf);
                debug!("server: client final buf len: {}", out.len());
                assert_eq!(out.len(), 9); //  4(array len)+ 5 bytes

                // need to explicity send out len
                let len = out.len() as i32;
                let mut len_buf = vec![];
                len.encode(&mut len_buf,0)?;
                sink.send(to_bytes(len_buf)).await?;

                sink.send(out).await?;
                /*
                debug!("server: sending 2nd value to client");
                let data2 = vec![0x20,0x11];
                await!(sink.send(to_bytes(data2)))?;
                // sleep for 100 ms to give client time
                debug!("wait for 50 ms to give receiver change to process");
                */
                future_helper::sleep(time::Duration::from_millis(50)).await;
                debug!("finishing. terminating server");
                return Ok(()) as Result<(), Error>;
            }

            Ok(()) as Result<(), Error>
        };

        let client_ft = async {
            debug!("client: sleep to give server chance to come up");
            sleep(time::Duration::from_millis(100)).await;
            debug!("client: trying to connect");
            let tcp_stream = AsyncTcpStream::connect(&addr).await?;
            debug!("client: got connection. waiting");
            let split: TcpStreamSplit<KfCodec> = tcp_stream.split();
            let mut stream = split.stream();
            if let Some(value) = stream.next().await {
                debug!("client :received first value from server");
                let mut bytes = value?;
                let values = bytes.take();
                debug!("client :received bytes of len: {}", values.len());
                assert_eq!(values.len(), 9, "total bytes is 9");

                let mut cursor = Cursor::new(values);
                let mut decoded_values = vec![];
                decoded_values
                    .decode(&mut cursor,0)
                    .expect("vector decoding failed");
                assert_eq!(decoded_values.len(), 5);
                assert_eq!(decoded_values[0], 1);
                assert_eq!(decoded_values[1], 2);
                debug!("all test pass");
            } else {
                assert!(false, "no first value received");
            }

            debug!("waiting for 2nd value");
            /*
            if let Some(value) = await!(stream.next()) {
                debug!("client: received 2nd value from server");
                let mut bytes = value?;
                let values = bytes.take();
                assert_eq!(values.len(),2);

            } else {
                assert!(false,"no second value received");
            }
            */

            debug!("finished client");

            Ok(()) as Result<(), Error>
        };

        let _rt = join(client_ft,server_ft).await;

        Ok(())
    }

}
