use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::{Attribute, Error, Field, Lit, Meta, NestedMeta, Type};

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

        let result = validate_versions(
            prop.attrs.min_version,
            prop.attrs.max_version,
            Some(&prop.field_name),
        );

        if let Some(err) = result {
            Err(syn::Error::new(field.span(), err))
        } else {
            Ok(prop)
        }
    }

    pub fn version_check_token_stream(&self, field_stream: TokenStream) -> TokenStream {
        let min = self.attrs.min_version;
        let field_name = &self.field_name;

        if let Some(max) = self.attrs.max_version {
            quote! {
                if (#min..=#max).contains(&version) {
                    #field_stream
                } else {
                    tracing::trace!("Field: <{}> is skipped because version: {} is outside min: {}, max: {}",stringify!(#field_name),version,#min,#max);
                }
            }
        } else {
            quote! {
                if version >= #min {
                    #field_stream
                } else {
                    tracing::trace!("Field: <{}> is skipped because version: {} is less than min: {}",stringify!(#field_name),version,#min);
                }
            }
        }
    }
}

impl UnnamedProp {
    pub fn from_ast(field: &Field) -> syn::Result<Self> {
        let attrs = PropAttrs::from_ast(&field.attrs)?;
        let field_type = field.ty.clone();
        let prop = UnnamedProp { field_type, attrs };

        let result = validate_versions(prop.attrs.min_version, prop.attrs.max_version, None);

        if let Some(err) = result {
            Err(syn::Error::new(field.span(), err))
        } else {
            Ok(prop)
        }
    }

    pub fn version_check_token_stream(&self, field_stream: TokenStream) -> TokenStream {
        let min = self.attrs.min_version;

        if let Some(max) = self.attrs.max_version {
            quote! {
                if (#min..=#max).contains(&version) {
                    #field_stream
                } else {
                    tracing::trace!("Field from tuple struct:is skipped because version: {} is outside min: {}, max: {}",version,#min,#max);
                }
            }
        } else {
            quote! {
                if version >= #min {
                    #field_stream
                } else {
                    tracing::trace!("Field from tuple struct: is skipped because version: {} is less than min: {}",version,#min);
                }
            }
        }
    }
}

pub fn validate_versions(min: i16, max: Option<i16>, field: Option<&str>) -> Option<String> {
    match (max, field) {
        // Print name in named fields
        (Some(max), Some(field)) if min > max => Some(format!(
            "On {field}, max version({max}) is less than min({min})."
        )),
        // No name to print in unnamed fields
        (Some(max), None) if min > max => {
            Some(format!("Max version({max}) is less than min({min})."))
        }
        (None, Some(field)) if min < 0 => {
            Some(format!("On {field} min version({min}) must be positive."))
        }
        (None, None) if min < 0 => Some(format!("Min version({min}) must be positive.")),
        _ => None,
    }
}

#[derive(Default, Clone)]
pub(crate) struct PropAttrs {
    pub varint: bool,
    /// Will default to 0 if not specified.
    /// Note: `None` is encoded as "-1" so it's i16.
    pub min_version: i16,
    /// Optional max version.
    /// The field won't be decoded from the buffer if it has a larger version than what is specified here.
    /// Note: `None` is encoded as "-1" so it's i16.
    pub max_version: Option<i16>,
    /// Sets this value to the field when it isn't present in the buffer.
    /// Example: `#[fluvio(default = "-1")]`
    pub default_value: Option<String>,
}

impl PropAttrs {
    pub fn from_ast(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut prop_attrs = Self::default();

        // Find all supported field level attributes in one go.
        for attribute in attrs.iter() {
            if attribute.path.is_ident("varint") {
                prop_attrs.varint = true;
            } else if attribute.path.is_ident("fluvio") {
                if let Ok(Meta::List(list)) = attribute.parse_meta() {
                    for kf_attr in list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(name_value)) = kf_attr {
                            if name_value.path.is_ident("min_version") {
                                if let Lit::Int(lit_int) = name_value.lit {
                                    prop_attrs.min_version = lit_int.base10_parse::<i16>()?;
                                }
                            } else if name_value.path.is_ident("max_version") {
                                if let Lit::Int(lit_int) = name_value.lit {
                                    prop_attrs.max_version = Some(lit_int.base10_parse::<i16>()?);
                                }
                            } else if name_value.path.is_ident("default") {
                                if let Lit::Str(lit_str) = name_value.lit {
                                    prop_attrs.default_value = Some(lit_str.value());
                                }
                            } else {
                                tracing::warn!(
                                    "#[fluvio({})] does nothing here.",
                                    name_value.to_token_stream().to_string(),
                                )
                            }
                        }
                    }
                }
            }
        }

        Ok(prop_attrs)
    }
}
