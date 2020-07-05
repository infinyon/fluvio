mod apis;
pub mod topics;
pub mod spu;
pub mod metadata;
pub mod versions;
mod request;
mod response;

pub use apis::*;
pub use request::*;
pub use response::*;

pub mod errors {
    pub use kf_protocol::api::FlvErrorCode;
}

/// Error from api call
#[derive(Debug)]
pub enum ApiError {
    Code(kf_protocol::api::FlvErrorCode, Option<String>),
    NoResourceFounded(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Code(code, msg_opt) => {
                if let Some(msg) = msg_opt {
                    write!(f, "{:#?} {}", code, msg)
                } else {
                    write!(f, "{:#?}", code)
                }
            }
            Self::NoResourceFounded(msg) => write!(f, "no resource founded {}", msg),
        }
    }
}
