use crate::ast::{
    container::ContainerAttributes, prop::Prop, r#enum::EnumProp, r#enum::FieldKind, DeriveItem,
};
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use std::str::FromStr;
use syn::{Ident, LitInt};

pub(crate) fn generate_encode_trait_impls(input: &DeriveItem) -> TokenStream {
    match &input {
        DeriveItem::Struct(kf_struct, _attrs) => {
            let ident = &kf_struct.struct_ident;
            let (impl_generics, ty_generics, where_clause) = kf_struct.generics.split_for_impl();
            let encoded_field_tokens = parse_struct_props_encoding(&kf_struct.props, ident);
            let size_field_tokens = parse_struct_props_size(&kf_struct.props, &ident);
            quote! {
                impl #impl_generics kf_protocol::Encoder for #ident #ty_generics #where_clause {
                    fn encode<T>(&self, dest: &mut T, version: kf_protocol::Version) -> Result<(),std::io::Error> where T: kf_protocol::bytes::BufMut {
                        log::trace!("encoding struct: {} version: {}",stringify!(#ident),version);
                        #encoded_field_tokens
                        Ok(())
                    }

                    fn write_size(&self, version: kf_protocol::Version) -> usize {
                        log::trace!("write size for struct: {} version {}",stringify!(#ident),version);
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
                impl #impl_generics kf_protocol::Encoder for #ident #ty_generics #where_clause {
                    fn encode<T>(&self, dest: &mut T, version: kf_protocol::Version) -> Result<(),std::io::Error> where T: kf_protocol::bytes::BufMut {
                        log::trace!("encoding struct: {} version: {}",stringify!(#ident),version);
                        #encoded_variant_tokens
                        Ok(())
                    }

                    fn write_size(&self, version: kf_protocol::Version) -> usize {
                        log::trace!("write size for struct: {} version {}",stringify!(#ident),version);
                        #size_variant_tokens
                    }
                }
            }
        }
    }
}

fn parse_struct_props_encoding(props: &[Prop], struct_ident: &Ident) -> TokenStream {
    let recurse = props.iter().map(|prop| {
        let fname = format_ident!("{}", prop.field_name);

        if prop.varint {
            quote! {
                log::trace!("encoding varint struct: <{}> field <{}> => {:?}",stringify!(#struct_ident),stringify!(#fname),&self.#fname);
                let result = self.#fname.encode_varint(dest);
                if result.is_err() {
                    log::error!("error varint encoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                    return result;
                }
            }
        } else {
            let base = quote! {
                log::trace!("encoding struct: <{}>, field <{}> => {:?}",stringify!(#struct_ident),stringify!(#fname),&self.#fname);
                let result = self.#fname.encode(dest,version);
                if result.is_err() {
                    log::error!("Error Encoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
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

fn parse_struct_props_size(props: &[Prop], struct_ident: &Ident) -> TokenStream {
    let recurse = props.iter().map(|prop| {
        let fname = format_ident!("{}", prop.field_name);
        if prop.varint {
            quote! {
                let write_size = self.#fname.var_write_size();
                log::trace!("varint write size: <{}>, field: <{}> is: {}",stringify!(#struct_ident),stringify!(#fname),write_size);
                len = len + write_size;
            }
        } else {
            let base = quote! {
                let write_size = self.#fname.write_size(version);
                log::trace!("write size: <{}> field: <{}> => {}",stringify!(#struct_ident),stringify!(#fname),write_size);
                len = len + write_size;
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
    let int_type = if let Some(int_type_name) = &attrs.repr_type_name {
        format_ident!("{}", int_type_name)
    } else {
        Ident::new("u8", Span::call_site())
    };
    let mut variant_expr = vec![];

    for (idx, prop) in props.iter().enumerate() {
        let id = &format_ident!("{}", prop.variant_name);
        // #[cfg(feature = "discriminant_panic")] {
        //     if prop.discriminant.is_some() && prop.tag.is_none() && !attrs.encode_discriminant {
        //         compiler_error!("feature[discriminant_panic]: either #[fluvio_kf(encode_discriminant)] or #[fluvio_kf(tag = ..)] is required on enum: {}", stringify!(#enum_ident));
        //     }
        // }
        let field_idx = if let Some(tag) = &prop.tag {
            if let Ok(literal) = TokenStream::from_str(tag) {
                literal
            } else {
                LitInt::new(&idx.to_string(), Span::call_site()).to_token_stream()
            }
        } else if attrs.encode_discriminant {
            if let Some(dsc) = &prop.discriminant {
                dsc.as_token_stream()
            } else {
                LitInt::new(&idx.to_string(), Span::call_site()).to_token_stream()
            }
        } else {
            LitInt::new(&idx.to_string(), Span::call_site()).to_token_stream()
        };
        let variant_code = match &prop.kind {
            FieldKind::Named(_expr) => {
                quote! { compiler_error!("named fields are not supported"); }
            }
            FieldKind::Unnamed(_expr) => {
                // TODO: handle cases like #enum_ident::#id(val1, val2, val3)
                quote! {
                    #enum_ident::#id(response) => {
                        let typ = #field_idx as #int_type;
                        typ.encode(dest,version)?;
                        response.encode(dest, version)?;
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
    let int_type = if let Some(int_type_name) = &attrs.repr_type_name {
        format_ident!("{}", int_type_name)
    } else {
        Ident::new("u8", Span::call_site())
    };
    let mut variant_expr: Vec<TokenStream> = vec![];

    for prop in props {
        let id = &format_ident!("{}", prop.variant_name);
        if let FieldKind::Unnamed(_) = &prop.kind {
            variant_expr.push(quote! {
                #enum_ident::#id(response) => { response.write_size(version) + std::mem::size_of::<#int_type>() },
            });
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
