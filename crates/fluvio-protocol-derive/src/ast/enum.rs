use crate::ast::prop::{NamedProp, UnnamedProp};
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{
    Error, Expr, ExprLit, ExprUnary, Fields, FieldsNamed, FieldsUnnamed, Generics, Ident, ItemEnum,
    Lit, Meta, NestedMeta, Variant,
};

use super::container::ContainerAttributes;

pub(crate) struct FluvioEnum {
    pub enum_ident: Ident,
    pub props: Vec<EnumProp>,
    pub generics: Generics,
}

impl FluvioEnum {
    pub fn from_ast(item: ItemEnum, attrs: &ContainerAttributes) -> syn::Result<Self> {
        let enum_ident = item.ident;
        let mut props = vec![];

        for variant in item.variants {
            let enum_prop = EnumProp::from_ast(variant.clone())?;

            if !attrs.encode_discriminant && enum_prop.tag.is_none() {
                return Err(Error::new(variant.span(), "You must provide `fluvio(encode_discriminant)` if `fluvio(tag)` is not provided"));
            }

            props.push(enum_prop);
        }

        let generics = item.generics;

        Ok(FluvioEnum {
            enum_ident,
            props,
            generics,
        })
    }
}

pub(crate) enum DiscrimantExpr {
    Lit(ExprLit),
    Unary(ExprUnary),
}

impl DiscrimantExpr {
    pub fn as_token_stream(&self) -> TokenStream {
        match self {
            Self::Lit(exp) => quote! { #exp },
            Self::Unary(exp) => quote! { #exp },
        }
    }
}

#[derive(Default)]
pub(crate) struct EnumProp {
    pub variant_name: String,
    pub tag: Option<String>,
    pub discriminant: Option<DiscrimantExpr>,
    pub kind: FieldKind,
}
impl EnumProp {
    pub fn from_ast(variant: Variant) -> syn::Result<Self> {
        let mut prop = EnumProp::default();
        let variant_ident = variant.ident.clone();
        prop.variant_name = variant_ident.to_string();
        // Find all supported field level attributes in one go.
        for attribute in &variant.attrs {
            if attribute.path.is_ident("fluvio") {
                if let Ok(Meta::List(list)) = attribute.parse_meta() {
                    for kf_attr in list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(name_value)) = kf_attr {
                            if name_value.path.is_ident("tag") {
                                if let Lit::Int(lit_int) = name_value.lit {
                                    prop.tag = Some(lit_int.base10_digits().to_owned());
                                }
                            }
                        }
                    }
                }
            }
        }

        prop.discriminant = if let Some((_, discriminant)) = variant.discriminant.clone() {
            match discriminant {
                Expr::Lit(elit) => Some(DiscrimantExpr::Lit(elit)),
                Expr::Unary(elit) => Some(DiscrimantExpr::Unary(elit)),
                _ => {
                    return Err(Error::new(
                        discriminant.span(),
                        "not supported discriminant type",
                    ))
                }
            }
        } else {
            None
        };

        if prop.discriminant.is_none() && prop.tag.is_none() {
            return Err(Error::new(
                variant.span(),
                "You must either provide a `tag` or a discriminant for enum types deriving Encode/Decode",
            ));
        }

        prop.kind = match &variant.fields {
            Fields::Named(struct_like) => {
                let props = struct_like
                    .named
                    .iter()
                    .map(NamedProp::from_ast)
                    .collect::<Result<Vec<_>, _>>()?;
                FieldKind::Named(struct_like.clone(), props)
            }
            Fields::Unnamed(tuple_like) => {
                let props = tuple_like
                    .unnamed
                    .iter()
                    .map(UnnamedProp::from_ast)
                    .collect::<Result<Vec<_>, _>>()?;
                FieldKind::Unnamed(tuple_like.clone(), props)
            }
            _ => FieldKind::Unit,
        };

        Ok(prop)
    }
}

pub(crate) enum FieldKind {
    Named(FieldsNamed, Vec<NamedProp>),
    Unnamed(FieldsUnnamed, Vec<UnnamedProp>),
    Unit,
}
impl Default for FieldKind {
    fn default() -> Self {
        Self::Unit
    }
}
