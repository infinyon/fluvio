mod api;
mod request;
mod response;

pub use self::api::*;
pub use self::response::*;
pub use self::request::*;

pub const MAX_BYTES: i32 = 52428800;

#[macro_export]
macro_rules! api_decode {
    ($api:ident,$req:ident,$src:expr,$header:expr) => {{
        use fluvio_protocol::Decoder;
        let request = $req::decode_from($src, $header.api_version())?;
        Ok($api::$req(RequestMessage::new($header, request)))
    }};
}
