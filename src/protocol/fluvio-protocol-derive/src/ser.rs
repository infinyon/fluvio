use crate::ast::{
    container::ContainerAttributes, prop::NamedProp, r#enum::EnumProp, r#enum::FieldKind,
    DeriveItem,
};
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use std::str::FromStr;
use syn::punctuated::Punctuated;
use syn::{Ident, LitInt, Token};

pub(crate) fn generate_encode_trait_impls(input: &DeriveItem) -> TokenStream {
    match &input {
        DeriveItem::Struct(kf_struct, _attrs) => {
            let ident = &kf_struct.struct_ident;
            let (impl_generics, ty_generics, where_clause) = kf_struct.generics.split_for_impl();
            let encoded_field_tokens = parse_struct_props_encoding(&kf_struct.props, ident);
            let size_field_tokens = parse_struct_props_size(&kf_struct.props, &ident);
            quote! {
                impl #impl_generics fluvio_protocol::Encoder for #ident #ty_generics #where_clause {
                    fn encode<T>(&self, dest: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error> where T: fluvio_protocol::bytes::BufMut {
                        tracing::trace!("encoding struct: {} version: {}",stringify!(#ident),version);
                        #encoded_field_tokens
                        Ok(())
                    }

                    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                        tracing::trace!("write size for struct: {} version {}",stringify!(#ident),version);
                        let mut len: usize = 0;
                        #size_field_tokens
                        len
                    }
                }
            }
        }
        DeriveItem::Enum(kf_enum, attrs) => {
            let ident = &kf_enum.enum_ident;
            let (impl_generics, ty_generics, where_clause) = kf_enum.generics.split_for_impl();
            let encoded_variant_tokens = parse_enum_variants_encoding(&kf_enum.props, ident, attrs);
            let size_variant_tokens = parse_enum_variants_size(&kf_enum.props, ident, attrs);
            quote! {
                impl #impl_generics fluvio_protocol::Encoder for #ident #ty_generics #where_clause {
                    fn encode<T>(&self, dest: &mut T, version: fluvio_protocol::Version) -> Result<(),std::io::Error> where T: fluvio_protocol::bytes::BufMut {
                        tracing::trace!("encoding struct: {} version: {}",stringify!(#ident),version);
                        #encoded_variant_tokens
                        Ok(())
                    }

                    fn write_size(&self, version: fluvio_protocol::Version) -> usize {
                        tracing::trace!("write size for struct: {} version {}",stringify!(#ident),version);
                        #size_variant_tokens
                    }
                }
            }
        }
    }
}

fn parse_struct_props_encoding(props: &[NamedProp], struct_ident: &Ident) -> TokenStream {
    let recurse = props.iter().map(|prop| {
        let fname = format_ident!("{}", prop.field_name);

        if prop.attrs.varint {
            quote! {
                tracing::trace!("encoding varint struct: <{}> field <{}> => {:?}",stringify!(#struct_ident),stringify!(#fname),&self.#fname);
                let result = self.#fname.encode_varint(dest);
                if result.is_err() {
                    tracing::error!("error varint encoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                    return result;
                }
            }
        } else {
            let base = quote! {
                tracing::trace!("encoding struct: <{}>, field <{}> => {:?}",stringify!(#struct_ident),stringify!(#fname),&self.#fname);
                let result = self.#fname.encode(dest,version);
                if result.is_err() {
                    tracing::error!("Error Encoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                    return result;
                }
            };

            prop.version_check_token_stream(base)
        }
    });

    quote! {
        #(#recurse)*
    }
}

fn parse_struct_props_size(props: &[NamedProp], struct_ident: &Ident) -> TokenStream {
    let recurse = props.iter().map(|prop| {
        let fname = format_ident!("{}", prop.field_name);
        if prop.attrs.varint {
            quote! {
                let write_size = self.#fname.var_write_size();
                tracing::trace!("varint write size: <{}>, field: <{}> is: {}",stringify!(#struct_ident),stringify!(#fname),write_size);
                len += write_size;
            }
        } else {
            let base = quote! {
                let write_size = self.#fname.write_size(version);
                tracing::trace!("write size: <{}> field: <{}> => {}",stringify!(#struct_ident),stringify!(#fname),write_size);
                len += write_size;
            };
            prop.version_check_token_stream(base)
        }
    });
    quote! {
        #(#recurse)*
    }
}

fn parse_enum_variants_encoding(
    props: &[EnumProp],
    enum_ident: &Ident,
    attrs: &ContainerAttributes,
) -> TokenStream {
    let int_type = match &attrs.repr_type_name {
        Some(int_type_name) => format_ident!("{}", int_type_name),
        _ => Ident::new("u8", Span::call_site()),
    };
    let mut variant_expr = vec![];

    for (idx, prop) in props.iter().enumerate() {
        let id = &format_ident!("{}", prop.variant_name);
        let field_idx = if let Some(tag) = &prop.tag {
            match TokenStream::from_str(tag) {
                Ok(literal) => literal,
                _ => LitInt::new(&idx.to_string(), Span::call_site()).to_token_stream(),
            }
        } else if attrs.encode_discriminant {
            match &prop.discriminant {
                Some(dsc) => dsc.as_token_stream(),
                _ => LitInt::new(&idx.to_string(), Span::call_site()).to_token_stream(),
            }
        } else {
            LitInt::new(&idx.to_string(), Span::call_site()).to_token_stream()
        };
        let variant_code = match &prop.kind {
            FieldKind::Named(_expr, props) => {
                // The "a, b, c, d" in Enum::Variant { a, b, c, d } => { ... }
                let fields = props
                    .iter()
                    .map(|it| format_ident!("{}", it.field_name))
                    .collect::<Vec<_>>();

                let encoding = fields.iter().map(|field| {
                    quote! {
                        #field .encode(dest, version)?;
                    }
                });

                quote! {
                    #enum_ident::#id { #(#fields),* } => {
                        let typ = #field_idx as #int_type;
                        typ.encode(dest, version)?;
                        #( #encoding )*
                    }
                }
            }
            FieldKind::Unnamed(_expr, props) => {
                // The "a, b, c, d" in Enum::Variant(a, b, c, d) => { ... }
                let fields = props
                    .iter()
                    .zip('a'..'z')
                    .map(|(_, b)| format_ident!("{}", b))
                    .collect::<Vec<_>>();

                let encoding = fields.iter().map(|field| {
                    quote! {
                        #field .encode(dest, version)?;
                    }
                });

                quote! {
                    #enum_ident::#id ( #(#fields),* ) => {
                        let typ = #field_idx as #int_type;
                        typ.encode(dest,version)?;
                        #(#encoding)*
                    },
                }
            }
            _ => quote! {
                #enum_ident::#id => {
                    let typ = #field_idx as #int_type;
                    typ.encode(dest,version)?;
                },
            },
        };
        variant_expr.push(variant_code);
    }
    quote! {
        match self {
            #(#variant_expr)*
        }
    }
}

fn parse_enum_variants_size(
    props: &[EnumProp],
    enum_ident: &Ident,
    attrs: &ContainerAttributes,
) -> TokenStream {
    let int_type = match &attrs.repr_type_name {
        Some(int_type_name) => format_ident!("{}", int_type_name),
        _ => Ident::new("u8", Span::call_site()),
    };
    let mut variant_expr: Vec<TokenStream> = vec![];

    for prop in props {
        let id = &format_ident!("{}", prop.variant_name);
        match &prop.kind {
            FieldKind::Unnamed(_expr, props) => {
                // The "a, b, c, d" in Enum::Variant(a, b, c, d) => { ... }
                let fields = props
                    .iter()
                    .zip('a'..'z')
                    .map(|(_, b)| format_ident!("{}", b))
                    .collect::<Vec<_>>();

                // [a.write_size(version), b.write_size(version), ...]
                let size_impls = fields
                    .iter()
                    .map(|field| {
                        quote! {
                            #field .write_size(version)
                        }
                    })
                    .collect::<Vec<_>>();

                // Join int size and field sizes, separated by `+` to sum them together
                // size_of::<#int_type>() + a.write_size() + b.write_size() + ...
                let size_sum: Punctuated<_, Token![+]> =
                    std::iter::once(quote! { std::mem::size_of::<#int_type>() })
                        .chain(size_impls)
                        .collect();

                let arm = quote! {
                    #enum_ident::#id ( #(#fields),* ) => {
                        #size_sum
                    },
                };
                variant_expr.push(arm);
            }
            FieldKind::Named(_expr, props) => {
                // The "a, b, c, d" in Enum::Variant(a, b, c, d) => { ... }
                let fields = props
                    .iter()
                    .map(|it| format_ident!("{}", it.field_name))
                    .collect::<Vec<_>>();

                // [a.write_size(version), b.write_size(version), ...]
                let size_impls = fields
                    .iter()
                    .map(|field| {
                        quote! {
                            #field .write_size(version)
                        }
                    })
                    .collect::<Vec<_>>();

                // Join int size and field sizes, separated by `+` to sum them together
                // size_of::<#int_type>() + a.write_size() + b.write_size() + ...
                let size_sum: Punctuated<_, Token![+]> =
                    std::iter::once(quote! { std::mem::size_of::<#int_type>() })
                        .chain(size_impls)
                        .collect();

                let arm = quote! {
                    #enum_ident::#id { #(#fields),* } => {
                        #size_sum
                    },
                };
                variant_expr.push(arm);
            }
            _ => (),
        }
    }

    if variant_expr.is_empty() {
        quote! {
            std::mem::size_of::<#int_type>()
        }
    } else {
        quote! {
            match self {
                #(#variant_expr)*
            }
        }
    }
}
