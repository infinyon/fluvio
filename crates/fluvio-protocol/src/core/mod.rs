mod buffer;
mod decoder;
mod encoder;
mod varint;
mod zerocopy;

pub use self::decoder::Decoder;
pub use self::decoder::DecoderVarInt;
pub use self::encoder::Encoder;
pub use self::encoder::EncoderVarInt;
pub type Version = i16;
