use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::Attribute;
use syn::Data;
use syn::DataStruct;
use syn::DeriveInput;
use syn::Fields;
use syn::Ident;

use crate::util::find_int_from_meta;
use crate::util::find_string_from_meta;

use super::util::find_attr;

pub(crate) fn generate_request_traits(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;

    let api_trait = generate_request_trait_impl(name, &input.attrs);

    quote! {
        #api_trait
    }
}

/// generate implementation for decoding kf protocol
pub(crate) fn parse_and_generate_api(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;

    match input.data {
        Data::Struct(ref data) => generate_api(data, name),
        _ => unimplemented!(),
    }
}

fn generate_api(data: &DataStruct, name: &Ident) -> TokenStream {
    let encoder = generate_encoder(data, name);

    quote! {
        #encoder
    }
}

fn generate_encoder(data: &DataStruct, name: &Ident) -> TokenStream {
    match data.fields {
        Fields::Named(ref fields) => {
            let fields_code = fields.named.iter().map(|f| {
                quote! {
                    #f,
                }
            });

            let definition = quote! {

                #[derive(Encoder,Decoder,RequestApi,Debug)]
                #[fluvio(default)]
                pub struct #name {
                    #(#fields_code)*
                }

            };

            let methods = fields.named.iter().map(|f| {
                let fname = &f.ident.as_ref().unwrap();
                let ty = &f.ty;

                let new_name = format!("set_{fname}");
                let setter_name = Ident::new(&new_name, Span::call_site());

                quote! {

                    #[allow(dead_code)]
                    #[inline]
                    pub fn #fname(&self) -> &#ty {
                        &self.#fname
                    }

                    #[allow(dead_code)]
                    #[inline]
                    pub fn #setter_name(&mut self, val: #ty) {
                        self.#fname = val;
                    }
                }
            });

            let accessor = quote! {

                impl #name {

                    #(#methods)*

                }
            };

            quote! {
                #definition

                #accessor
            }
        }
        _ => unimplemented!(),
    }
}

fn generate_request_trait_impl(name: &Ident, attrs: &[Attribute]) -> TokenStream {
    // check if we have api version
    let version_meta = if let Some(version) = find_attr(attrs, "fluvio") {
        version
    } else {
        return quote! {};
    };
    let api_key = match find_int_from_meta(&version_meta, "api_key") {
        Ok(data) => data,
        Err(err) => return err.to_compile_error(),
    };
    let min_version = match find_int_from_meta(&version_meta, "api_min_version") {
        Ok(data) => data,
        Err(err) => return err.to_compile_error(),
    };

    let response = match find_string_from_meta(&version_meta, "response") {
        Ok(data) => data,
        Err(err) => return err.to_compile_error(),
    };

    let response_type = Ident::new(&response.value(), Span::call_site());

    let max_version = if let Ok(max_version) = find_int_from_meta(&version_meta, "api_max_version")
    {
        if max_version < min_version {
            syn::Error::new(
                version_meta.span(),
                "max version must be greater than or equal to min version",
            )
            .to_compile_error()
        } else {
            quote! {
                const MAX_API_VERSION: i16 = #max_version as i16;
            }
        }
    } else {
        quote! {}
    };

    quote! {

        impl Request for #name {

            const API_KEY: u16 = #api_key as u16;

            const MIN_API_VERSION: i16 = #min_version as i16;

            #max_version

            type Response = #response_type;

        }

    }
}
