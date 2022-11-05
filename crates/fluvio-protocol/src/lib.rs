// Allow fluvio_protocol_derive to use fluvio_protocol exports
// See https://github.com/rust-lang/rust/issues/56409
extern crate self as fluvio_protocol;

pub mod core;

#[cfg(feature = "derive")]
pub use fluvio_protocol_derive::{Encoder, Decoder, FluvioDefault, RequestApi};

#[cfg(feature = "derive")]
pub use fluvio_protocol_derive as derive;

#[cfg(feature = "api")]
pub mod api;

#[cfg(feature = "codec")]
pub mod codec;

#[cfg(feature = "record")]
pub mod record;

#[cfg(feature = "link")]
pub mod link;

#[cfg(feature = "fixture")]
pub mod fixture;

#[cfg(all(unix, feature = "store"))]
pub mod store;

pub use self::core::ByteBuf;
pub use self::core::Decoder;
pub use self::core::DecoderVarInt;
pub use self::core::Encoder;
pub use self::core::EncoderVarInt;
pub use self::core::Version;

pub use bytes;
