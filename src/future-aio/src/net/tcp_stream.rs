
use async_std::net::TcpStream;
use async_std::net::TcpListener;


pub type AsyncTcpListener = TcpListener;
pub type AsyncTcpStream = TcpStream;




#[cfg(test)]
mod tests {

    use std::io::Error;
    use std::net::SocketAddr;
    use std::thread;
    use std::time;

    use bytes::BufMut;
    use bytes::Bytes;
    use bytes::BytesMut;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use futures::future::join;
    use flv_future_core::sleep;
    use log::debug;
    use futures_codec::BytesCodec;
    use futures_codec::Framed;
    use async_std::prelude::*;

    use flv_future_core::test_async;
    use flv_future_core::spawn;

    use super::AsyncTcpListener;
    use super::AsyncTcpStream;


    fn to_bytes(bytes: Vec<u8>) -> Bytes {
        let mut buf = BytesMut::with_capacity(bytes.len());
        buf.put_slice(&bytes);
        buf.freeze()
    }

    #[test_async]
    async fn future_join() -> Result<(), Error> {
       
        // with join, futures are dispatched on same thread
        // since ft1 starts first and
        // blocks on thread, it will block future2
        // should see ft1,ft1,ft2,ft2

        //let mut ft_id = 0;

        let ft1 = async {
           
           debug!("ft1: starting sleeping for 1000ms");
           // this will block ft2.  both ft1 and ft2 share same thread
           thread::sleep(time::Duration::from_millis(1000)); 
           debug!("ft1: woke from sleep");
         //  ft_id = 1;      
            Ok(()) as Result<(),()>
        };

        let ft2 = async {
            debug!("ft2: starting sleeping for 500ms");
            thread::sleep(time::Duration::from_millis(500)); 
            debug!("ft2: woke up");
         //   ft_id = 2;            
            Ok(()) as Result<(), ()>
        };

        let core_threads = num_cpus::get().max(1);
        debug!("num threads: {}",core_threads);
        let _rt = join(ft1,ft2).await;
        assert!(true);
        Ok(())
    }



    #[test_async]
    async fn future_spawn() -> Result<(), Error> {
       
        // with spawn, futures are dispatched on separate thread
        // in this case, thread sleep on ft1 won't block 
        // should see  ft1, ft2, ft2, ft1

        let ft1 = async {
           
           debug!("ft1: starting sleeping for 1000ms");
           thread::sleep(time::Duration::from_millis(1000)); // give time for server to come up
           debug!("ft1: woke from sleep");            
        };

        let ft2 = async {
           
            debug!("ft2: starting sleeping for 500ms");
            thread::sleep(time::Duration::from_millis(500)); 
            debug!("ft2: woke up");
        };

        let core_threads = num_cpus::get().max(1);
        debug!("num threads: {}",core_threads);

        spawn(ft1);
        spawn(ft2);
        // wait for all futures complete
        thread::sleep(time::Duration::from_millis(2000));

        assert!(true);
       

        Ok(())
    }



    #[test_async]
    async fn test_async_tcp() -> Result<(), Error> {
        let addr = "127.0.0.1:9998".parse::<SocketAddr>().expect("parse");

        let server_ft = async {
           
             debug!("server: binding");
             let listener = AsyncTcpListener::bind(&addr).await?;
             debug!("server: successfully binding. waiting for incoming");
             let mut incoming = listener.incoming();
             while let Some(stream) = incoming.next().await {
                 debug!("server: got connection from client");
                 let tcp_stream = stream?;
                 let mut framed = Framed::new(tcp_stream,BytesCodec{});
                 debug!("server: sending values to client");
                 let data = vec![0x05, 0x0a, 0x63];
                 framed.send(to_bytes(data)).await?;
                 sleep(time::Duration::from_micros(1)).await;
                 debug!("server: sending 2nd value to client");
                 let data2 = vec![0x20,0x11]; 
                 framed.send(to_bytes(data2)).await?;
                 return Ok(()) as Result<(),Error>

            }
            
            Ok(()) as Result<(), Error>
        };

        let client_ft = async {
           
            debug!("client: sleep to give server chance to come up");
            sleep(time::Duration::from_millis(100)).await;
            debug!("client: trying to connect");
            let tcp_stream = AsyncTcpStream::connect(&addr).await?;
            let mut framed = Framed::new(tcp_stream,BytesCodec{});
            debug!("client: got connection. waiting");
            if let Some(value) = framed.next().await {
                debug!("client :received first value from server");
                let bytes = value?;
                let values = bytes.take(3).into_inner();
                assert_eq!(values[0],0x05);
                assert_eq!(values[1],0x0a);
                assert_eq!(values[2],0x63);
                assert_eq!(values.len(),3);
            } else {
                assert!(false,"no value received");
            }

            if let Some(value) = framed.next().await {
                debug!("client: received 2nd value from server");
                let bytes = value?;
                let values = bytes.take(2).into_inner();
                assert_eq!(values.len(),2);

            } else {
                assert!(false,"no value received");
            }

            
            Ok(()) as Result<(), Error>
        };


        let _rt = join(client_ft,server_ft).await;

        Ok(())
    }

}
