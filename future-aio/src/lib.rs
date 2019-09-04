#![feature(generators)]

#[cfg(not(feature = "tokio2"))]
mod compat;
pub mod fs;

#[cfg(not(feature = "tokio2"))]
mod io_util_1;
#[cfg(feature = "tokio2")]
mod io_util_3;


mod write;
mod zero_copy;
pub mod net;

#[cfg(feature = "tokio2")]
use self::io_util_3::asyncify;
#[cfg(not(feature = "tokio2"))]
use self::io_util_1::asyncify;

pub use self::write::AsyncWrite2;
pub use self::write::WriteBufAll;
pub use self::zero_copy::ZeroCopyWrite;
pub use self::zero_copy::SendFileError;

pub use bytes::Bytes;
pub use bytes::BytesMut;
pub use bytes::BufMut;