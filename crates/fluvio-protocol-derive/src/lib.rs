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

#[proc_macro_derive(Decoder, attributes(varint, trace, fluvio))]
pub fn fluvio_decode(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_decode_trait_impls(&input);

    expanded.into()
}

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

#[proc_macro_derive(RequestApi, attributes(varint, fluvio))]
pub fn fluvio_request(tokens: TokenStream) -> TokenStream {
    let inputs = parse_macro_input![tokens as syn::DeriveInput];

    let expanded = generate_request_traits(&inputs);
    expanded.into()
}

#[proc_macro_derive(FluvioDefault, attributes(fluvio))]
pub fn fluvio_default(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input![tokens as ast::DeriveItem];
    let expanded = generate_default_trait_impls(&input);

    expanded.into()
}
