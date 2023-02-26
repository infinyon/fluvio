extern crate proc_macro;

mod api;
mod ast;
mod de;
mod ser;
mod util;

use self::api::generate_request_traits;
use self::api::parse_and_generate_api;
use self::de::generate_decode_trait_impls;
use self::de::generate_default_trait_impls;
use self::ser::generate_encode_trait_impls;

use proc_macro::TokenStream;
use syn::parse_macro_input;

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
///     First,
///     Second,
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
#[proc_macro_derive(Decoder, attributes(varint, trace, fluvio))]
pub fn fluvio_decode(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_decode_trait_impls(&input);

    expanded.into()
}

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
#[proc_macro_derive(Encoder, attributes(varint, trace, fluvio))]
pub fn fluvio_encode(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_encode_trait_impls(&input);

    expanded.into()
}

#[proc_macro]
pub fn fluvio_api(tokens: TokenStream) -> TokenStream {
    let inputs = parse_macro_input![tokens as syn::DeriveInput];

    let expanded = parse_and_generate_api(&inputs);
    expanded.into()
}

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
#[proc_macro_derive(RequestApi, attributes(varint, fluvio))]
pub fn fluvio_request(tokens: TokenStream) -> TokenStream {
    let inputs = parse_macro_input![tokens as syn::DeriveInput];

    let expanded = generate_request_traits(&inputs);
    expanded.into()
}

/// Custom derive for generating default structure
///
/// Example:
///
/// ```
/// use fluvio_protocol::derive::FluvioDefault;
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
#[proc_macro_derive(FluvioDefault, attributes(fluvio))]
pub fn fluvio_default(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_default_trait_impls(&input);

    expanded.into()
}
