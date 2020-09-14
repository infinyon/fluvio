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

/// Custom derive for decoding structure or enum from bytes using Kafka protocol format.
/// This assumes all fields implement kafka decode traits.
///
/// # Examples
///
/// ```
/// use kf_protocol::Decoder;
/// use kf_protocol::derive::Decode;
///
/// #[derive(Decode)]
/// pub struct SimpleRecord {
///     val: u8
/// }
///
/// let data = [
///     0x04
/// ];
///
/// let record = SimpleRecord::decode_from(&mut Cursor::new(&data),0).expect("decode");
/// assert_eq!(record.val,4);
///
/// ```
///
///
/// Decode applies to either Struct of Enum.  For enum, it implements `TryFrom` trait.  
/// Currently it only supports integer variants.  
///
/// So this works
///
/// ```
/// #[derive(Decode)]
/// pub enum ThreeChoice {
///     First = 1,
///     Second = 2,
///     Third = 3
/// }
/// ```
///
/// Also, enum without integer literal works as well
/// ```
/// #[derive(Decode)]
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
/// Decode support container and field level attributes.
/// Container level applies to struct.
/// For field attributes
/// * `#[varint]` force decode using varint format.
/// * `#fluvio_kf(min_version = <version>)]` decodes only if version is equal or greater than min_version
/// * `#fluvio_kf(max_version = <version>)]`decodes only if version is less or equal than max_version
///
#[proc_macro_derive(Decode, attributes(varint, fluvio_kf))]
pub fn kf_decode(tokens: TokenStream) -> TokenStream {
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
/// use kf_protocol::Encoder;
/// use kf_protocol::derive::Encode;
///
/// #[derive(Encode)]
/// pub struct SimpleRecord {
///     val: u8
/// }
///
/// let data = vec![];
///
/// let record = SimpleRecord { val: 4};
/// recprd.encode(&mut data,0);
///     
/// assert_eq!(data[0],4);
///
/// ```
///
///
/// Encode applys to either Struct of Enum.  
///
///
/// Encode respects version attributes.  See Decode derive.
///
///
///
#[proc_macro_derive(Encode, attributes(varint, fluvio_kf))]
pub fn kf_encode(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_encode_trait_impls(&input);

    expanded.into()
}

#[proc_macro]
pub fn kf_api(tokens: TokenStream) -> TokenStream {
    let inputs = parse_macro_input![tokens as syn::DeriveInput];

    let expanded = parse_and_generate_api(&inputs);
    expanded.into()
}

/// Custom derive for implementing Request trait.
/// This derives requires `fluvio_kf`
///
/// # Examples
///
/// ```
/// use kf_protocol::derive::Decode;
/// use kf_protocol::derive::Encode;
/// use kf_protocol::api::Request;
/// use kf_protocol::derive::RequestApi;
///
/// #[fluvio_kf(default,api_min_version = 5, api_max_version = 6, api_key = 10, response = "SimpleResponse")]
/// #[derive(Request,Encode,Decode,Default)]
/// pub struct SimpleRequest {
///     val: u8
/// }
///
///
/// #[derive(Encode,Decode,Default)]
/// #[fluvio_kf(default)]
/// pub struct TestResponse {
///     pub value: i8,
/// }
///
/// ```
///
/// RequestApi derives respects following attributes in `fluvio_kf`
///
/// * `api_min_version`:  min version that API supports.  This is required
/// * `api_max_version`:  max version that API supports.  This is optional.
/// * `api_key`:  API number.  This is required
/// * `response`:  Response struct.  This is required
///
#[proc_macro_derive(RequestApi, attributes(varint, fluvio_kf))]
pub fn kf_request(tokens: TokenStream) -> TokenStream {
    let inputs = parse_macro_input![tokens as syn::DeriveInput];

    let expanded = generate_request_traits(&inputs);
    expanded.into()
}

/// Custom derive for generating default structure
///
///
/// Example:
///
/// ```
/// #[derive(KfDefault)]
/// #[fluvio_kf(default)]
/// pub struct SimpleRecord {
///     #[fluvio_kf(default = "-1" )]
///     val: u8
/// }
///
/// let record = SimpleRecord::default;
/// assert_eq!(record.val,-1);
/// ```
///
/// `default` assignment can be any Rust expression.
#[proc_macro_derive(KfDefault, attributes(fluvio_kf))]
pub fn kf_default(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_default_trait_impls(&input);

    expanded.into()
}
