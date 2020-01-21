#![feature(generators)]

pub mod fs;
pub mod sync;


mod write;
mod zero_copy;
pub mod net;


pub use self::write::AsyncWrite2;
pub use self::write::WriteBufAll;
pub use self::zero_copy::ZeroCopyWrite;
pub use self::zero_copy::SendFileError;

pub use bytes::Bytes;
pub use bytes::BytesMut;
pub use bytes::BufMut;

pub mod io {
    pub use async_std::io::Stdin;
    pub use async_std::io::Stdout;
    pub use async_std::io::stdin;
    pub use async_std::io::ReadExt;
    pub use async_std::io::Read;
    pub use async_std::io::BufRead;
    pub use async_std::io::BufReader;
    pub use async_std::io::prelude::BufReadExt;
    pub use async_std::println;
}
