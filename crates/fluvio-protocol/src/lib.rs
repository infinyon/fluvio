// Allow fluvio_protocol_derive to use fluvio_protocol exports
// See https://github.com/rust-lang/rust/issues/56409
extern crate self as fluvio_protocol;

pub mod core;

#[cfg(feature = "derive")]
pub use inner_deriver::*;

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

#[cfg(feature = "derive")]
mod inner_deriver {

    /// Custom derive for encoding structure or enum to bytes using Kafka protocol format.
    /// This assumes all fields(or enum variants) implement kafka encode traits.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluvio_protocol::Encoder;
    ///
    /// #[derive(Encoder)]
    /// pub struct SimpleRecord {
    ///     val: u8
    /// }
    ///
    /// let mut data = vec![];
    ///
    /// let record = SimpleRecord { val: 4 };
    /// record.encode(&mut data,0);
    ///     
    /// assert_eq!(data[0],4);
    /// ```
    ///
    /// Encoder applies to either Struct of Enum.  
    ///
    /// Encoder respects version attributes.  See Decoder derive.
    pub use fluvio_protocol_derive::Encoder;

    /// Custom derive for decoding structure or enum from bytes using fluvio protocol format.
    /// This assumes all fields implement fluvio decode traits.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use fluvio_protocol::Decoder;
    ///
    /// #[derive(Default, Decoder)]
    /// pub struct SimpleRecord {
    ///     val: u8
    /// }
    ///
    /// let data = [
    ///     0x04
    /// ];
    ///
    /// let record = SimpleRecord::decode_from(&mut Cursor::new(&data),0).expect("decode");
    /// assert_eq!(record.val, 4);
    /// ```
    ///
    ///
    /// Decoder applies to either Struct of Enum.  For enum, it implements `TryFrom` trait.  
    /// Currently it only supports integer variants.  
    ///
    /// So this works
    ///
    /// ```
    /// # use fluvio_protocol::Decoder;
    /// # impl Default for ThreeChoice { fn default() -> Self { unimplemented!() } }
    /// #[derive(Decoder)]
    /// #[fluvio(encode_discriminant)]
    /// pub enum ThreeChoice {
    ///     First = 1,
    ///     Second = 2,
    ///     Third = 3
    /// }
    /// ```
    ///
    /// Also, enum without integer literal works as well
    ///
    /// ```
    /// # use fluvio_protocol::Decoder;
    /// # impl Default for ThreeChoice { fn default() -> Self { unimplemented!() } }
    /// #[derive(Decoder)]
    /// pub enum ThreeChoice {
    ///     #[fluvio(tag = 0)]
    ///     First,
    ///     #[fluvio(tag = 1)]
    ///     Second,
    ///     #[fluvio(tag = 2)]
    ///     Third
    /// }
    /// ```
    ///
    /// In this case, 1 is decoded as First, 2 as Second, 3 as Third.
    ///
    /// Currently, mixing enum variants are not supported.
    ///
    ///
    /// Decoder support container and field level attributes.
    /// Container level applies to struct.
    /// For field attributes
    /// * `#[varint]` force decode using varint format.
    /// * `#[trace]` print out debug information during decoding
    /// * `#fluvio(min_version = <version>)]` decodes only if version is equal or greater than min_version
    /// * `#fluvio(max_version = <version>)]`decodes only if version is less or equal than max_version
    ///
    pub use fluvio_protocol_derive::Decoder;

    /// Custom derive for implementing Request trait.
    /// This derives requires `fluvio`
    ///
    /// # Examples
    ///
    /// ```
    /// use fluvio_protocol::{Encoder, Decoder};
    /// use fluvio_protocol::api::Request;
    /// use fluvio_protocol::derive::RequestApi as Request;
    ///
    /// #[fluvio(default,api_min_version = 5, api_max_version = 6, api_key = 10, response = "SimpleResponse")]
    /// #[derive(Debug, Default, Encoder, Decoder, Request)]
    /// pub struct SimpleRequest {
    ///     val: u8
    /// }
    ///
    /// #[derive(Debug, Default, Encoder, Decoder)]
    /// #[fluvio(default)]
    /// pub struct SimpleResponse {
    ///     pub value: i8,
    /// }
    /// ```
    ///
    /// RequestApi derives respects following attributes in `fluvio`
    ///
    /// * `api_min_version`:  min version that API supports.  This is required
    /// * `api_max_version`:  max version that API supports.  This is optional.
    /// * `api_key`:  API number.  This is required
    /// * `response`:  Response struct.  This is required
    ///
    ///
    pub use fluvio_protocol_derive::RequestApi;

    /// Custom derive for generating default structure
    ///
    /// Example:
    ///
    /// ```
    /// use fluvio_protocol::FluvioDefault;
    ///
    /// #[derive(FluvioDefault)]
    /// #[fluvio(default)]
    /// pub struct SimpleRecord {
    ///     #[fluvio(default = "12")]
    ///     val: u8
    /// }
    ///
    /// let record = SimpleRecord::default();
    /// assert_eq!(record.val, 12);
    /// ```
    ///
    /// `default` assignment can be any Rust expression.
    pub use fluvio_protocol_derive::FluvioDefault;
}
