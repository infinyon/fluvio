#![allow(clippy::assign_op_pattern)]

pub mod server;
pub mod client;
pub mod errors {
    pub use dataplane_protocol::ErrorCode;
}
