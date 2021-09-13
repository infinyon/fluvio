pub mod core;

#[cfg(feature = "derive")]
pub use fluvio_protocol_derive::{Encoder, Decoder};

#[cfg(feature = "derive")]
pub use fluvio_protocol_derive as derive;

#[cfg(feature = "api")]
pub mod api;

#[cfg(feature = "codec")]
pub mod codec;

#[cfg(all(unix, feature = "store"))]
pub mod store;

pub use self::core::Decoder;
pub use self::core::DecoderVarInt;
pub use self::core::Encoder;
pub use self::core::EncoderVarInt;
pub use self::core::Version;
