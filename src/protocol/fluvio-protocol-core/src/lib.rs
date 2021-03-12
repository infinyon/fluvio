mod decoder;
mod encoder;
mod varint;
mod zerocopy;

pub use self::decoder::Decoder;
pub use self::decoder::DecoderVarInt;
pub use self::encoder::Encoder;
pub use self::encoder::EncoderVarInt;

pub mod bytes {
    pub use bytes::Buf;
    pub use bytes::BufMut;
    pub use bytes::Bytes;
    pub use bytes::BytesMut;
}

pub type Version = i16;
