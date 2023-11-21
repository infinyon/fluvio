use std::str::FromStr;

use proc_macro2::{Ident, TokenStream};
use quote::quote;

use syn::spanned::Spanned;
use syn::{parse_quote, Attribute, Error, Field, Type};

use crate::util::{get_expr_value_from_meta, get_lit_str, parse_attributes, parse_attributes_data};

#[derive(Clone)]
pub(crate) struct NamedProp {
    pub field_name: String,
    pub field_type: Type,
    pub attrs: PropAttrs,
}

#[derive(Clone)]
pub(crate) struct UnnamedProp {
    pub field_type: Type,
    pub attrs: PropAttrs,
}

impl NamedProp {
    pub fn from_ast(field: &Field) -> syn::Result<Self> {
        let field_ident = if let Some(ident) = &field.ident {
            ident.clone()
        } else {
            return Err(Error::new(
                field.span(),
                "Named field must have an `ident`.",
            ));
        };
        let field_name = field_ident.to_string();
        let field_type = field.ty.clone();
        let attrs = PropAttrs::from_ast(&field.attrs)?;

        let prop = NamedProp {
            field_name,
            field_type,
            attrs,
        };

        Ok(prop)
    }

    pub fn version_check_token_stream(
        &self,
        field_stream: TokenStream,
        trace: bool,
    ) -> TokenStream {
        let field_name = &self.field_name;
        let min_version = &self.attrs.min_version;
        let min = prop_attrs_type_value(min_version, None);

        if *&self.attrs.max_version.is_some() {
            let max = prop_attrs_type_value(&self.attrs.max_version, None);
            let trace = if trace {
                quote! {
                    else {
                        tracing::trace!("Field: <{}> is skipped because version: {} is outside min: {}, max: {}",stringify!(#field_name),version,#min,#max);
                    }
                }
            } else {
                quote! {}
            };
            quote! {
                if (#min..=#max).contains(&version) {
                    #field_stream
                }
                #trace
            }
        } else {
            let trace = if trace {
                quote! {
                    else {
                        tracing::trace!("Field: <{}> is skipped because version: {} is less than min: {}",stringify!(#field_name),version,#min);
                    }
                }
            } else {
                quote! {}
            };
            quote! {
                if version >= #min {
                    #field_stream
                }
                #trace
            }
        }
    }
}

impl UnnamedProp {
    pub fn from_ast(field: &Field) -> syn::Result<Self> {
        let attrs = PropAttrs::from_ast(&field.attrs)?;
        let field_type = field.ty.clone();
        let prop = UnnamedProp { field_type, attrs };

        Ok(prop)
    }

    pub fn version_check_token_stream(
        &self,
        field_stream: TokenStream,
        trace: bool,
    ) -> TokenStream {
        let min = prop_attrs_type_value(&self.attrs.min_version, None);
        if *&self.attrs.max_version.is_some() {
            let max = prop_attrs_type_value(&self.attrs.max_version , None);
            let trace = if trace {
                quote! {
                    else {
                        tracing::trace!("Field from tuple struct:is skipped because version: {} is outside min: {}, max: {}",version,#min,#max);
                    }
                }
            } else {
                quote! {}
            };

            quote! {
                if (#min..=#max).contains(&version) {
                    #field_stream
                }
                #trace
            }
        } else {
            let trace = if trace {
                quote! {
                    else {
                        tracing::trace!("Field from tuple struct: is skipped because version: {} is less than min: {}",version,#min);
                    }
                }
            } else {
                quote! {}
            };

            quote! {
                if version >= #min {
                    #field_stream
                }
                #trace
            }
        }
    }
}
/// Convert the values to TokenStream which will be ready to use variable value
///
/// #Example
/// ````
/// // Function as a literal
/// fn test() -> i16 { 1 }
/// #[fluvio(min_version = "test()")]
/// ````
/// To use the value from the test() function:
/// ````
/// let func_value = prop_attrs_type_value(prop_attr_type, None)
/// ````
/// To set a specific type you can do this:
/// ````
/// let ident_type = Ident::new("u8", Span::call_site());
/// let func_value = prop_attrs_type_value(prop_attr_type, Some(&ident_type))
/// ````
///
pub fn prop_attrs_type_value(
    attrs_type: &Option<PropAttrsType>,
    ident_type: Option<&Ident>,
) -> TokenStream {
    if let Some(attr) = attrs_type {
        match &attr {
            PropAttrsType::Lit(data) => parse_quote!(#data),
            PropAttrsType::Fn(data) => parse_quote!(#data()),
            PropAttrsType::Int(data) => {
                if let Some(itype) = ident_type {
                    TokenStream::from_str(&format!("{}_{}", data, itype)).unwrap()
                } else {
                    // By default it's i16, because most places use it
                    parse_quote!(#data)
                }
            }
            PropAttrsType::None => parse_quote!(0),
        }
    } else {
        parse_quote!(0)
    }
}
/// A type that will handle the values passed in properties
/// and convert them later on to TokenStream.
///
/// Using this type allows you to pass values multiple ways:
/// # Example
///
/// ```
/// // Constant as a path
/// const TEST: i16 = 1;
/// #[fluvio(min_version = TEST)]
/// ```
///
/// ```
/// // Constant as a literal
/// const TEST: i16 = 1;
/// #[fluvio(min_version = "TEST")]
/// ```
///
/// ```
/// // Function as a literal
/// fn test() -> i16 { 1 }
/// #[fluvio(min_version = "test()")]
/// ```
///
/// ```
/// // Int
/// #[fluvio(min_version = 1)]
/// ```
///
/// None has a default Int value of 0 which is set in prop_attrs_type_value
#[derive(Default, Clone)]
pub enum PropAttrsType {
    Lit(Ident),
    Fn(Ident),
    Int(i16),
    #[default]
    None,
}
#[derive(Default, Clone)]
pub(crate) struct PropAttrs {
    pub varint: bool,
    /// Will default to 0 if not specified.
    pub min_version: Option<PropAttrsType>,
    /// Optional max version.
    /// The field won't be decoded from the buffer if it has a larger version than what is specified here.
    pub max_version: Option<PropAttrsType>,
    /// Sets this value to the field when it isn't present in the buffer.
    /// Example: `#[fluvio(default = "-1")]`
    pub default_value: Option<String>,
    pub ignorable: Option<bool>,
}
impl PropAttrs {
    pub fn from_ast(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut prop_attrs = Self::default();

        for attr in attrs {
            if let Some(ident) = attr.path().get_ident() {
                if ident == "varint" {
                    prop_attrs.varint = true;
                }
            }
        }

        parse_attributes!(attrs.iter(), "fluvio", meta,
            "min_version", prop_attrs.min_version => {
                let value = get_expr_value_from_meta(&meta)?;
                prop_attrs.min_version = Some(value);
            }
            "max_version", prop_attrs.max_version => {
                let value = get_expr_value_from_meta(&meta)?;
                prop_attrs.max_version = Some(value);
            }
            "default", prop_attrs.default_value =>  {
                let (expr, attr_span, attr_name) = parse_attributes_data(&meta);
                let value = get_lit_str(&attr_name, &expr, attr_span)?;
                prop_attrs.default_value = Some(value.value());
            }
            "ignorable", prop_attrs.ignorable => {
                prop_attrs.ignorable = Some(true);
            }
        );

        Ok(prop_attrs)
    }
}
