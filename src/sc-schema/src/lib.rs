mod apis;
pub mod topic;
pub mod spu;
pub mod spg;
pub mod partition;
pub mod versions;
pub mod objects;
mod request;
mod response;

pub use apis::*;
pub use request::*;
pub use response::*;
pub use admin::*;

pub mod errors {
    pub use dataplane_protocol::ErrorCode;
}

pub mod core {
    pub use fluvio_controlplane_metadata::core::*;
}

pub mod store {
    pub use fluvio_controlplane_metadata::store::*;
}

/// Error from api call
#[derive(Debug)]
pub enum ApiError {
    Code(dataplane_protocol::ErrorCode, Option<String>),
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

mod admin {

    use dataplane_protocol::api::Request;

    pub trait AdminRequest: Request {}
}
