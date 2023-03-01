use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use std::str::FromStr;
use syn::punctuated::Punctuated;
use syn::Ident;
use syn::LitInt;
use syn::Token;

use crate::ast::add_bounds;
use crate::ast::prop::UnnamedProp;
use crate::ast::r#struct::FluvioStructProps;
use crate::ast::FluvioBound;
use crate::ast::{
    container::ContainerAttributes, prop::NamedProp, r#enum::EnumProp, r#enum::FieldKind,
    DeriveItem,
};

pub(crate) fn generate_decode_trait_impls(input: &DeriveItem) -> TokenStream {
    match &input {
        DeriveItem::Struct(kf_struct, attrs) => {
            // TODO: struct level attrs is not used.
            let field_tokens =
                generate_struct_fields(&kf_struct.props(), kf_struct.struct_ident(), attrs);
            let ident = &kf_struct.struct_ident();
            let generics = add_bounds(kf_struct.generics().clone(), attrs, FluvioBound::Decoder);
            let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
            quote! {
                impl #impl_generics fluvio_protocol::Decoder for #ident #ty_generics #where_clause {
                    fn decode<T>(&mut self, src: &mut T,version: fluvio_protocol::Version) -> Result<(),std::io::Error> where T: fluvio_protocol::bytes::Buf {
                      //  tracing::trace!("decoding struct: {}",stringify!(#ident));
                        #field_tokens
                        Ok(())
                    }
                }
            }
        }
        DeriveItem::Enum(kf_enum, attrs) => {
            let ident = &kf_enum.enum_ident;
            let generics = add_bounds(kf_enum.generics.clone(), attrs, FluvioBound::Decoder);
            let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
            let int_type = if let Some(int_type_name) = &attrs.repr_type_name {
                format_ident!("{}", int_type_name)
            } else {
                Ident::new("u8", Span::call_site())
            };
            let enum_tokens = generate_decode_enum_impl(&kf_enum.props, &int_type, ident, attrs);
            let try_enum = generate_try_enum_from_kf_enum(&kf_enum.props, &int_type, ident, attrs);
            let res = quote! {
                impl #impl_generics fluvio_protocol::Decoder for #ident #ty_generics #where_clause {
                    fn decode<T>(&mut self, src: &mut T,version: fluvio_protocol::Version) -> Result<(),std::io::Error> where T: fluvio_protocol::bytes::Buf {
                        #enum_tokens
                        Ok(())
                    }
                }

                #try_enum
            };
            res
        }
    }
}

pub(crate) fn generate_struct_fields(
    props: &FluvioStructProps,
    struct_ident: &Ident,
    attr: &ContainerAttributes,
) -> TokenStream {
    match props {
        FluvioStructProps::Named(named_fields) => {
            generate_struct_named_fields(named_fields, struct_ident, attr)
        }
        FluvioStructProps::Unnamed(unnamed_fields) => {
            generate_struct_unnamed_fields(unnamed_fields, struct_ident, attr)
        }
    }
}

pub(crate) fn generate_struct_named_fields(
    props: &[NamedProp],
    struct_ident: &Ident,
    attr: &ContainerAttributes,
) -> TokenStream {
    let recurse = props.iter().map(|prop| {
        let fname = format_ident!("{}", prop.field_name);
        if prop.attrs.varint {
            if attr.trace {
                quote! {
                    tracing::trace!("start decoding varint field <{}>", stringify!(#fname));
                    let result = self.#fname.decode_varint(src);
                    if result.is_ok() {
                        tracing::trace!("decoding ok varint <{}> => {:?}",stringify!(#fname),&self.#fname);
                    } else {
                        tracing::trace!("decoding varint error <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                        return result;
                    }
                }
            } else {
                quote! {
                    self.#fname.decode_varint(src)?;
                }
            }
        } else {
            let base = if attr.trace {
                quote! {
                    tracing::trace!("start decoding struct: <{}> field: <{}>",stringify!(#struct_ident),stringify!(#fname));
                    let result = self.#fname.decode(src,version);
                    if result.is_ok() {
                        tracing::trace!("decoding struct: <{}> field: <{}> => {:#?}",stringify!(#struct_ident),stringify!(#fname),&self.#fname);
                    } else {
                        tracing::trace!("error decoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                        return result;
                    }
                }
             } else {
                    quote! {
                        self.#fname.decode(src,version)?;
                    }
            };

            prop.version_check_token_stream(base, attr.trace)
        }
    });
    quote! {
        #(#recurse)*
    }
}

pub(crate) fn generate_struct_unnamed_fields(
    props: &[UnnamedProp],
    struct_ident: &Ident,
    attrs: &ContainerAttributes,
) -> TokenStream {
    let recurse = props.iter().enumerate().map(|(idx, prop)| {
        let field_idx = syn::Index::from(idx);
        if prop.attrs.varint {
            if attrs.trace {
                quote! {
                    tracing::trace!("start decoding varint field <{}>", stringify!(#idx));
                    let result = self.#field_idx.decode_varint(src);
                    if result.is_ok() {
                        tracing::trace!("decoding ok varint <{}> => {:?}",stringify!(#idx),&self.#field_idx);
                    } else {
                        tracing::trace!("decoding varint error <{}> ==> {}",stringify!(#idx),result.as_ref().unwrap_err());
                        return result;
                    }
                }
            } else {
                quote! {
                    self.#field_idx.decode_varint(src)?;
                }
            }
        } else {
            let base = if attrs.trace {
                quote! {
                    tracing::trace!("start decoding struct: <{}> field: <{}>",stringify!(#struct_ident),stringify!(#idx));
                    let result = self.#field_idx.decode(src,version);
                    if result.is_ok() {
                        tracing::trace!("decoding struct: <{}> field: <{}> => {:#?}",stringify!(#struct_ident),stringify!(#idx),&self.#field_idx);
                    } else {
                        tracing::trace!("error decoding <{}> ==> {}",stringify!(#idx),result.as_ref().unwrap_err());
                        return result;
                    }
                }
            }else {
                quote! {
                    self.#field_idx.decode(src,version)?;
                }
            };

            prop.version_check_token_stream(base, attrs.trace)
        }
    });
    quote! {
        #(#recurse)*
    }
}

fn generate_decode_enum_impl(
    props: &[EnumProp],
    int_type: &Ident,
    enum_ident: &Ident,
    attrs: &ContainerAttributes,
) -> TokenStream {
    let mut arm_branches = vec![];
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

        let arm_code = match &prop.kind {
            FieldKind::Unnamed(_, props) => {
                let (decode, fields): (Vec<_>, Punctuated<_, Token![,]>) = props
                    .iter()
                    .enumerate()
                    .map(|(idx, prop)| {
                        let var_ident = format_ident!("res_{}", idx);
                        let var_ty = &prop.field_type;
                        // Type will be inferred when used to construct parent
                        let decode = quote! {
                            let mut #var_ident: #var_ty = Default::default();
                            #var_ident.decode(src, version)?;
                        };
                        (decode, var_ident)
                    })
                    .unzip();

                quote! {
                    #field_idx => {
                        #(#decode)*

                        *self = Self::#id ( #fields );
                    }
                }
            }
            FieldKind::Named(_, props) => {
                let (decode, fields): (Vec<_>, Punctuated<_, Token![,]>) = props
                    .iter()
                    .map(|prop| {
                        let var_ident = format_ident!("{}", &prop.field_name);
                        let var_ty = &prop.field_type;
                        // Type will be inferred when used to construct parent
                        let decode = quote! {
                            let mut #var_ident: #var_ty = Default::default();
                            #var_ident.decode(src, version)?;
                        };
                        (decode, var_ident)
                    })
                    .unzip();

                quote! {
                    #field_idx => {
                        #(#decode)*

                        *self = Self::#id { #fields };
                    }
                }
            }
            FieldKind::Unit => {
                quote! {
                    #field_idx => {
                        *self = Self::#id;
                    }
                }
            }
        };

        arm_branches.push(arm_code);
    }

    arm_branches.push(quote! {
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown {} type {}", stringify!(#enum_ident), typ)
            ));
        }
    });

    let output = quote! {
        let mut typ: #int_type = 0;
        typ.decode(src, version)?;
        tracing::trace!("decoded type: {}", typ);

        match typ {
            #(#arm_branches),*
        }
    };

    output
}

fn generate_try_enum_from_kf_enum(
    props: &[EnumProp],
    int_type: &Ident,
    enum_ident: &Ident,
    attrs: &ContainerAttributes,
) -> TokenStream {
    // If #[fluvio(encode_discriminant)] is used, this is an int-enum
    // If it is not, it may be carrying data, so we cannot have impl TryFrom<int>
    if !attrs.encode_discriminant {
        return quote! {};
    }

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
            FieldKind::Named(expr, _props) => {
                let mut decode_variant_fields = vec![];
                for (idx, field) in expr.named.iter().enumerate() {
                    let field_ident = &field.ident;
                    let var_ident = format_ident!("res_{}", idx);
                    decode_variant_fields.push(quote! {
                        let mut #var_ident = #field_ident::default();
                        #var_ident.decode(src, version)?;
                    });
                }
                let mut variant_construction_params = vec![];
                for (idx, _) in decode_variant_fields.iter().enumerate() {
                    let var_ident = format_ident!("res_{}", idx);
                    variant_construction_params.push(quote! {#var_ident, });
                }
                quote! {
                    #field_idx => {
                        #(#decode_variant_fields)*
                        Ok(Self::#id{#(#variant_construction_params)*}),
                    }
                }
            }
            FieldKind::Unnamed(expr, _props) => {
                let mut decode_variant_fields = vec![];
                for (idx, field) in expr.unnamed.iter().enumerate() {
                    let field_ident = &field.ident;
                    let var_ident = format_ident!("res_{}", idx);
                    decode_variant_fields.push(quote! {
                        let mut #var_ident = #field_ident::default();
                        #var_ident.decode(src, version)?;
                    });
                }
                let mut variant_construction_params = vec![];
                for (idx, _) in decode_variant_fields.iter().enumerate() {
                    let var_ident = format_ident!("res_{}", idx);
                    variant_construction_params.push(quote! {#var_ident, });
                }
                quote! {
                    #field_idx => {
                        #(#decode_variant_fields)*
                        Ok(Self::#id(#(#variant_construction_params)*)),
                    }
                }
            }
            _ => quote! {
                #field_idx => Ok(Self::#id),
            },
        };
        variant_expr.push(variant_code);
    }
    variant_expr.push(quote! {
        _ => return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown {} type {}", stringify!(#enum_ident), typ)
            ))
    });

    quote! {
        impl std::convert::TryFrom<#int_type> for #enum_ident {
            type Error = std::io::Error;

            fn try_from(value: #int_type) -> Result<Self, Self::Error> {
                let typ = stringify!(#int_type);
                match value  {
                    #(#variant_expr)*
                }
            }
        }
    }
}

pub(crate) fn generate_default_trait_impls(input: &DeriveItem) -> TokenStream {
    match &input {
        DeriveItem::Struct(kf_struct, attrs) => {
            let ident = &kf_struct.struct_ident();
            let field_tokens = generate_default_impls(&kf_struct.props());
            let generics = add_bounds(kf_struct.generics().clone(), attrs, FluvioBound::Default);
            let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
            quote! {
                impl #impl_generics Default for #ident #ty_generics #where_clause {
                    fn default() -> Self {
                        Self {
                            #field_tokens
                        }
                    }
                }
            }
        }
        DeriveItem::Enum(_, _) => quote! {},
    }
}

pub(crate) fn generate_default_impls(props: &FluvioStructProps) -> TokenStream {
    match props {
        FluvioStructProps::Named(named_fields) => generate_default_impls_named_fields(named_fields),
        FluvioStructProps::Unnamed(unnamed_fields) => {
            generate_default_impls_unnamed_fields(unnamed_fields)
        }
    }
}
pub(crate) fn generate_default_impls_named_fields(props: &[NamedProp]) -> TokenStream {
    let recurse = props.iter().map(|prop| {
        let fname = format_ident!("{}", prop.field_name);
        if let Some(def) = &prop.attrs.default_value {
            if let Ok(liter) = TokenStream::from_str(def) {
                quote! {
                    #fname: #liter,
                }
            } else {
                quote! {
                    #fname: std::default::Default::default(),
                }
            }
        } else {
            quote! {
                #fname: std::default::Default::default(),
            }
        }
    });
    quote! {
        #(#recurse)*
    }
}

pub(crate) fn generate_default_impls_unnamed_fields(props: &[UnnamedProp]) -> TokenStream {
    let recurse = props.iter().enumerate().map(|(idx, prop)| {
        let field_idx = syn::Index::from(idx);

        if let Some(def) = &prop.attrs.default_value {
            if let Ok(liter) = TokenStream::from_str(def) {
                quote! {
                    #field_idx: #liter,
                }
            } else {
                quote! {
                    #field_idx: std::default::Default::default(),
                }
            }
        } else {
            quote! {
                #field_idx: std::default::Default::default(),
            }
        }
    });
    quote! {
        #(#recurse)*
    }
}
