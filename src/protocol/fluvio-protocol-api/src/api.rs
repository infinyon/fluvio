use std::default::Default;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::io::Read;
use std::fmt::Debug;
use std::fmt;
use std::convert::TryFrom;

use log::trace;
use log::debug;

use kf_protocol::Decoder;
use kf_protocol::Encoder;
use kf_protocol::bytes::Buf;
use kf_protocol_derive::Decode;
use kf_protocol_derive::Encode;


pub trait Request: Encoder + Decoder + Debug {

    const API_KEY: u16;

    const DEFAULT_API_VERSION: i16 = 0;
    const MIN_API_VERSION: i16 = 0;
    const MAX_API_VERSION: i16 = -1;

    type Response: Encoder + Decoder + Debug ;

}


pub trait KfRequestMessage: Sized + Default 

{
        type ApiKey: Decoder + Debug ;
       
        fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self,IoError>
        where
                Self: Default + Sized,
                Self::ApiKey: Sized,
                T: Buf;

        
        fn decode_from<T>(src: &mut T) -> Result<Self,IoError>
                where T: Buf,
                    
        {
                let header = RequestHeader::decode_from(src,0)?;
                Self::decode_with_header(src,header)

        }

        fn decode_from_file<P: AsRef<Path>>(file_name: P) -> Result<Self,IoError> {

                debug!("decoding from file: {:#?}", file_name.as_ref());
                let mut f = File::open(file_name)?;
                let mut buffer: [u8; 1000] = [0; 1000];

                f.read(&mut buffer)?;

                let data = buffer.to_vec();
                let mut src = Cursor::new(&data);

                let mut size: i32 = 0;
                size.decode(&mut src,0)?;
                trace!("decoded request size: {} bytes", size);

                if src.remaining() < size as usize {
                         return Err(IoError::new(
                                ErrorKind::UnexpectedEof,
                                "not enought bytes for request message",
                        ));
                }


                Self::decode_from(&mut src)
        }

}

pub trait KfApiKey: Sized + Encoder + Decoder + TryFrom<u16> {
    
}




#[derive(Debug, Encode, Decode, Default)]
pub struct RequestHeader {
    api_key: u16,
    api_version: i16,
    correlation_id: i32,
    client_id: String,
}

impl fmt::Display for RequestHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"api: {} client: {}",self.api_key,self.client_id)
    }
}



impl RequestHeader {

    
    pub fn new(api_key: u16) -> Self {
        // TODO: generate random client id
        Self::new_with_client(api_key,"dummy".to_owned())
    }

    pub fn new_with_client<T>(api_key: u16,client_id: T) -> Self 
        where T: Into<String>
    {
        RequestHeader {
            api_key,
            api_version: 1,
            correlation_id: 1,

            client_id: client_id.into()
        }
    }

    pub fn api_key(&self) -> u16 {
        self.api_key
    }

    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    pub fn set_api_version(&mut self,version: i16) -> &mut Self {
        self.api_version = version;
        self
    }

    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    pub fn set_correlation_id(&mut self,id: i32) -> &mut Self  {
        self.correlation_id = id;
        self
    }

    pub fn client_id(&self) -> &String {
        &self.client_id
    }

    pub fn set_client_id<T>(&mut self,client_id: T) -> &mut Self
        where T: Into<String>
    {
        self.client_id = client_id.into();
        self
    }
    
}

impl From<&RequestHeader> for i32 {
    fn from(header: &RequestHeader) -> i32 {
        header.correlation_id()
    }
}
