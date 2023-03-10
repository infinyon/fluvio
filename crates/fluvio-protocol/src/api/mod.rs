mod request;
mod response;

pub use self::response::*;
pub use self::request::*;

pub const MAX_BYTES: i32 = 52428800;

pub use crate::api_decode;

#[macro_export]
macro_rules! api_decode {
    ($api:ident,$req:ident,$src:expr,$header:expr) => {{
        use fluvio_protocol::Decoder;
        let request = $req::decode_from($src, $header.api_version())?;
        Ok($api::$req(RequestMessage::new($header, request)))
    }};
}

pub use common::*;
mod common {

    use std::fmt::{self, Debug, Display, Formatter};
    use std::path::Path;
    use std::fs::File;
    use std::io::{Cursor, Error as IoError, ErrorKind, Read};
    use std::convert::TryFrom;

    use bytes::Buf;
    use tracing::{debug, trace};

    use crate::{Encoder, Decoder};

    const fn max(a: i16, b: i16) -> i16 {
        if a > b {
            a
        } else {
            b
        }
    }

    pub trait Request: Encoder + Decoder + Debug {
        const API_KEY: u16;

        const DEFAULT_API_VERSION: i16 = 0;
        const MIN_API_VERSION: i16 = max(Self::DEFAULT_API_VERSION - 1, 0); // by default, only suport last version
        const MAX_API_VERSION: i16 = Self::DEFAULT_API_VERSION;

        type Response: Encoder + Decoder + Debug;
    }

    pub trait ApiMessage: Sized + Default {
        type ApiKey: Decoder + Debug;

        fn decode_with_header<T>(src: &mut T, header: RequestHeader) -> Result<Self, IoError>
        where
            Self: Default + Sized,
            Self::ApiKey: Sized,
            T: Buf;

        fn decode_from<T>(src: &mut T) -> Result<Self, IoError>
        where
            T: Buf,
        {
            let header = RequestHeader::decode_from(src, 0)?;
            Self::decode_with_header(src, header)
        }

        fn decode_from_file<P: AsRef<Path>>(file_name: P) -> Result<Self, IoError> {
            debug!("decoding from file: {:#?}", file_name.as_ref());
            let mut f = File::open(file_name)?;
            let mut buffer: [u8; 1000] = [0; 1000];

            f.read_exact(&mut buffer)?;

            let data = buffer.to_vec();
            let mut src = Cursor::new(&data);

            let mut size: i32 = 0;
            size.decode(&mut src, 0)?;
            trace!("decoded request size: {} bytes", size);

            if src.remaining() < size as usize {
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    "not enough bytes for request message",
                ));
            }

            Self::decode_from(&mut src)
        }
    }

    pub trait ApiKey: Sized + Encoder + Decoder + TryFrom<u16> {}

    #[derive(Debug, Encoder, Decoder, Default, Clone)]
    pub struct RequestHeader {
        api_key: u16,
        api_version: i16,
        correlation_id: i32,
        client_id: String,
    }

    impl fmt::Display for RequestHeader {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "api: {} client: {}", self.api_key, self.client_id)
        }
    }

    impl RequestHeader {
        pub fn new(api_key: u16) -> Self {
            // TODO: generate random client id
            Self::new_with_client(api_key, "dummy".to_owned())
        }

        pub fn new_with_client<T>(api_key: u16, client_id: T) -> Self
        where
            T: Into<String>,
        {
            RequestHeader {
                api_key,
                api_version: 1,
                correlation_id: 1,

                client_id: client_id.into(),
            }
        }

        pub fn api_key(&self) -> u16 {
            self.api_key
        }

        pub fn api_version(&self) -> i16 {
            self.api_version
        }

        pub fn set_api_version(&mut self, version: i16) -> &mut Self {
            self.api_version = version;
            self
        }

        pub fn correlation_id(&self) -> i32 {
            self.correlation_id
        }

        pub fn set_correlation_id(&mut self, id: i32) -> &mut Self {
            self.correlation_id = id;
            self
        }

        pub fn client_id(&self) -> &String {
            &self.client_id
        }

        pub fn set_client_id<T>(&mut self, client_id: T) -> &mut Self
        where
            T: Into<String>,
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

    #[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
    #[non_exhaustive]
    pub enum RequestKind {
        #[default]
        #[fluvio(tag = 0)]
        Produce,
    }

    impl Display for RequestKind {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }
}
