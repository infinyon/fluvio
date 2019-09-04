#![feature(generators)]

mod backflow;
mod error;
mod pooling;
mod socket;
mod stream;
mod sink;
mod sink_pool;
mod file_fetch;
mod file_produce;

#[cfg(test)]
pub mod test_request;

pub use self::error::KfSocketError;
pub use self::socket::KfSocket;
pub use pooling::SocketPool;
pub use sink_pool::SinkPool;
pub use sink_pool::SharedSinkPool;
pub use stream::KfStream;
pub use sink::KfSink;
pub use sink::ExclusiveKfSink;
pub use file_fetch::FilePartitionResponse;
pub use file_fetch::FileFetchResponse;
pub use file_fetch::FileTopicResponse;
pub use file_fetch::KfFileFetchRequest;
pub use file_produce::FileProduceRequest;
pub use file_produce::FileTopicRequest;
pub use file_produce::FilePartitionRequest;
pub use file_fetch::KfFileRecordSet;

use std::net::SocketAddr;
use std::io::Error as IoError;

use log::trace;

use future_aio::Bytes;
use future_aio::BytesMut;
use future_aio::fs::AsyncFileSlice;
use kf_protocol::Version;
use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use kf_protocol::Encoder;


/// send request and respons to socket addr
pub async fn send_and_receive<R>(
    addr: SocketAddr,
    request: &RequestMessage<R>,
) -> Result<ResponseMessage<R::Response>, KfSocketError>
where
    R: Request
{
    let mut client = KfSocket::connect(&addr).await?;

    let msgs: ResponseMessage<R::Response> = client.send(&request).await?;

    Ok(msgs)
}



pub enum StoreValue<'a> {
    Bytes(Bytes),
    FileSlice(&'a AsyncFileSlice)
}

impl <'a>std::fmt::Debug for StoreValue<'a> {

    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
           StoreValue::Bytes(bytes) => write!(f, "StoreValue:Bytes wiht len: {}", bytes.len()),
           StoreValue::FileSlice(slice) => write!(f, "StoreValue:FileSlice: {:#?}",slice),
        }
        
    }
}



pub trait FileWrite: Encoder {

    fn file_encode<'a: 'b,'b>(&'a self, src: &mut BytesMut, _data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError>  {
        self.encode(src,version)
    }

}


impl<M> FileWrite for Vec<M> where M:  FileWrite,
{

    fn file_encode<'a: 'b,'b>(&'a self, src: &mut BytesMut, data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError>
    {
        let len: i32 = self.len() as i32;
        len.encode(src,version)?;
        for v in self {
            v.file_encode(src,data,version)?;
        }
        Ok(())
    }
}



impl <P>FileWrite for ResponseMessage<P>  where P: FileWrite + Default   {

    fn file_encode<'a: 'b,'b>(&'a self, dest: &mut BytesMut, data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError> {
        
        trace!("file encoding response message");
        let len = self.write_size(version) as i32;
        trace!("encoding response len: {}", len);
        len.encode(dest,version)?;

        trace!("encoding response corre _id: {}",self.correlation_id);
        self.correlation_id.encode(dest,version)?;

        trace!("encoding response");
        self.response.file_encode(dest,data,version)?;
        Ok(())
    }  
}



impl <R>FileWrite for RequestMessage<R> where R: FileWrite + Default  + Request {

    fn file_encode<'a: 'b,'b>(&'a self, dest: &mut BytesMut, data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError> {
        
        trace!("file encoding response message");
        let len = self.write_size(version) as i32;
        trace!("file encoding response len: {}", len);
        len.encode(dest,version)?;

        trace!("file encoding header");
        self.header.encode(dest,version)?;

        trace!("encoding response");
        self.request.file_encode(dest,data,version)?;
        Ok(())
    }  
}

