mod api;
mod request;
mod response;


pub mod core {
    pub use fluvio_protocol::*;
}

pub mod derive {
    pub use fluvio_protocol_derive::*;
}


pub use self::api::*;
pub use self::response::*;
pub use self::request::*;



//pub use self::group_protocol_metadata::ProtocolMetadata;
//pub use self::group_protocol_metadata::Metadata;
//pub use self::group_assigment::GroupAssignment;
//pub use self::group_assigment::Assignment;
// pub use self::common::*;


pub const MAX_BYTES: i32 = 52428800;

#[macro_export]
macro_rules! api_decode {
    ($api:ident,$req:ident,$src:expr,$header:expr) => {{
        use fluvio_protocol::Decoder;
        let request = $req::decode_from($src, $header.api_version())?;
        Ok($api::$req(RequestMessage::new($header, request)))
    }};
}



