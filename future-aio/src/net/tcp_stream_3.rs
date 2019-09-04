use std::io::Error as IoError;
/// wrapper over tokio stream
/// should be compatible with romio tcp stream but
/// wrapper over tokio tcp to make it usable now
use std::net::SocketAddr;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;

use futures::stream::StreamExt;
use futures::stream::SplitSink;
use futures::Stream;
use futures::stream::SplitStream;

use bytes::Bytes;
use bytes::BytesMut;
use bytes::BufMut;
use tokio_2::codec::Framed as TkFramed;
use tokio_2::codec::Encoder as TkEncoder;
use tokio_2::codec::Decoder as TkDecoder;
use tokio_2::net::TcpStream as TkTcpStream;
use tokio_2::net::TcpListener as TkTcpListner;

use crate::ZeroCopyWrite;


#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;



pub struct AsyncTcpListener(TkTcpListner);

impl AsyncTcpListener {
    pub fn bind(addr: &SocketAddr) -> Result<Self, IoError> {
        let listener = TkTcpListner::bind(addr)?;
        Ok(AsyncTcpListener(listener))
    }

    pub fn incoming(self) -> impl Stream<Item = Result<AsyncTcpStream, IoError>> {
        self.0
            .incoming()
            .map(|tcp_stream | tcp_stream.map(|inner| inner.into()))
    }
}


/// This should be same as Future TcpStream like Romeo
/// but use tokio io for compatibility
pub struct AsyncTcpStream(TkTcpStream);


impl Display for AsyncTcpStream {


    fn fmt(&self, f: &mut Formatter) -> FmtResult {

        if let Ok(local_addr) =  self.local_addr() {
             write!(f, "local: {} ",local_addr)?;
        }
        if let Ok(peer_addr) = self.peer_addr() {
            write!(f, "peer: {} ",peer_addr)?;
        }

        write!(f,"fd: {}",self.as_raw_fd())
       
    }
}


impl From<TkTcpStream> for AsyncTcpStream {
    fn from(tcp: TkTcpStream) -> Self {
        AsyncTcpStream(tcp)
    }
}

impl AsyncTcpStream {
    pub async fn connect(addr: &SocketAddr) -> Result<AsyncTcpStream, IoError> {
        let inner_tcp = TkTcpStream::connect(addr).await?;
        Ok(inner_tcp.into())
    }

    pub fn local_addr(&self) -> Result<SocketAddr, IoError> {
        self.0.local_addr()
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, IoError> {
        self.0.peer_addr()
    }

    
    pub fn split<C>(self) -> TcpStreamSplit<C> 
        where Self: Into<TcpStreamSplit<C>>, C: Unpin + TkEncoder 
    {
        self.into()
    }

}



impl AsRawFd for AsyncTcpStream  {

    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }

}

impl ZeroCopyWrite for AsyncTcpStream {}


pub type TcpStreamSplitStream<C> = SplitStream<TkFramed<TkTcpStream,C>>;
pub type TcpStreamSplitSink<C> = SplitSink<TkFramed<TkTcpStream,C>,<C as TkEncoder>::Item>;
//unsafe impl <C>Sync for TcpStreamSplitSink<C>{}


pub struct TcpStreamSplit<C> 
    where C: TkEncoder + Unpin,    
{
    sink:  TcpStreamSplitSink<C>,
    stream: TcpStreamSplitStream<C>
}

impl <C> Into<TcpStreamSplit<C>>  for AsyncTcpStream  where C: Default + TkDecoder + TkEncoder + Unpin {

    fn into(self) -> TcpStreamSplit<C> {
        
        let (sink, stream) = C::default().framed(self.0).split();
        TcpStreamSplit {
            sink,
            stream
        }
    }
}

impl <C>TcpStreamSplit<C> 
    where C: TkEncoder + Unpin
{

    pub fn get_sink(&self) -> &SplitSink<TkFramed<TkTcpStream,C>,C::Item>{
        &self.sink
    }


    pub fn get_mut_sink(&mut self) -> &mut SplitSink<TkFramed<TkTcpStream,C>,C::Item>{
        &mut self.sink
    }

    pub fn get_mut_stream(&mut self) -> &mut TcpStreamSplitStream<C> {
        &mut self.stream
    }

    pub fn sink(self) -> SplitSink<TkFramed<TkTcpStream,C>,C::Item> {
        self.sink
    }


    pub fn stream(self) -> TcpStreamSplitStream<C> {
        self.stream
    }


    // convert into tutple
    pub fn as_tuple(self) -> ( SplitSink<TkFramed<TkTcpStream,C>,C::Item>,TcpStreamSplitStream<C>) {
        (self.sink,self.stream)
    }
}

// borrowed from bytes codec but implemented default
#[derive(Default)]
pub struct SimpleCodec(());

impl TkDecoder for SimpleCodec {
    type Item = BytesMut;
    type Error = IoError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>,IoError> {
        if buf.len() > 0 {
            let len = buf.len();
            Ok(Some(buf.split_to(len)))
        } else {
            Ok(None)
        }
    }
}

impl TkEncoder for SimpleCodec {
    type Item = Bytes;
    type Error = IoError;

    fn encode(&mut self, data: Bytes, buf: &mut BytesMut) -> Result<(),IoError> {
        buf.reserve(data.len());
        buf.put(data);
        Ok(())
    }
}



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
    use future_helper::sleep;
    use log::debug;

    use future_helper::test_async;
    use future_helper::spawn;

    use super::AsyncTcpListener;
    use super::AsyncTcpStream;
    use super::TcpStreamSplit;
    use super::SimpleCodec;

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
             let listener = AsyncTcpListener::bind(&addr)?;
             debug!("server: successfully binding. waiting for incoming");
             let mut incoming = listener.incoming();
             while let Some(stream) = incoming.next().await {
                 debug!("server: got connection from client");
                 let tcp_stream = stream?;
                 let split: TcpStreamSplit<SimpleCodec> = tcp_stream.split();
                 let mut sink = split.sink();
                 debug!("server: seding values to client");
                 let data = vec![0x05, 0x0a, 0x63];
                 sink.send(to_bytes(data)).await?;
                 sleep(time::Duration::from_micros(1)).await;
                 debug!("server: sending 2nd value to client");
                 let data2 = vec![0x20,0x11];
                 sink.send(to_bytes(data2)).await?;
                 return Ok(()) as Result<(),Error>

            }
            
            Ok(()) as Result<(), Error>
        };

        let client_ft = async {
           
            debug!("client: sleep to give server chance to come up");
            sleep(time::Duration::from_millis(100)).await;
            debug!("client: trying to connect");
            let tcp_stream = AsyncTcpStream::connect(&addr).await?;
            debug!("client: got connection. waiting");
            let split: TcpStreamSplit<SimpleCodec> = tcp_stream.split();
            let mut stream = split.stream();
            if let Some(value) = stream.next().await {
                debug!("client :received first value from server");
                let mut bytes = value?;
                let values = bytes.take();
                assert_eq!(values[0],0x05);
                assert_eq!(values[1],0x0a);
                assert_eq!(values[2],0x63);
                assert_eq!(values.len(),3);
            } else {
                assert!(false,"no value received");
            }

            if let Some(value) = stream.next().await {
                debug!("client: received 2nd value from server");
                let mut bytes = value?;
                let values = bytes.take();
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
