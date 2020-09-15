use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::spanned::Spanned;
use syn::{Field, Lit, Meta, NestedMeta, Error};

#[derive(Default)]
pub(crate) struct Prop {
    pub field_name: String,
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

impl Prop {
    pub fn from_ast(field: &Field) -> syn::Result<Self> {
        let mut prop = Prop::default();
        let field_ident = if let Some(ident) = &field.ident {
            ident.clone()
        } else {
            return Err(Error::new(field.span(), "Named field must have an `ident`."));
        };
        prop.field_name = field_ident.to_string();
        // Find all supported field level attributes in one go.
        for attribute in &field.attrs {
            if attribute.path.is_ident("varint") {
                prop.varint = true;
            } else if attribute.path.is_ident("fluvio") {
                if let Ok(Meta::List(list)) = attribute.parse_meta() {
                    for kf_attr in list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(name_value)) = kf_attr {
                            if name_value.path.is_ident("min_version") {
                                if let Lit::Int(lit_int) = name_value.lit {
                                    prop.min_version =
                                        lit_int.base10_parse::<i16>()?;
                                }
                            } else if name_value.path.is_ident("max_version") {
                                if let Lit::Int(lit_int) = name_value.lit {
                                    prop.max_version = Some(lit_int.base10_parse::<i16>()?);
                                }
                            } else if name_value.path.is_ident("default") {
                                if let Lit::Str(lit_str) = name_value.lit {
                                    prop.default_value = Some(lit_str.value());
                                }
                            } else {
                                log::warn!(
                                    "#[fluvio({})] does nothing on {}.",
                                    name_value.to_token_stream().to_string(),
                                    prop.field_name
                                )
                            }
                        }
                    }
                }
            }
        }
        if let Some(err) =
            Self::validate_versions(prop.min_version, prop.max_version, &prop.field_name)
        {
            Err(syn::Error::new(field.span(), err))
        } else {
            Ok(prop)
        }
    }

    pub fn validate_versions(min: i16, max: Option<i16>, field: &str) -> Option<String> {
        if let Some(max) = max {
            if min > max {
                Some(format!(
                    "On {}, max version({}) is less than min({}).",
                    field, max, min
                ))
            } else {
                None
            }
        } else if min < 0 {
            Some(format!(
                "On {} min version({}) must be positive.",
                field, min
            ))
        } else {
            None
        }
    }

    pub fn version_check_token_stream(&self, field_stream: TokenStream) -> TokenStream {
        let min = self.min_version;
        let field_name = &self.field_name;

        if let Some(max) = self.max_version {
            quote! {
                if version >= #min && version <= #max {
                    #field_stream
                } else {
                    log::trace!("Field: <{}> is skipped because version: {} is outside min: {}, max: {}",stringify!(#field_name),version,#min,#max);
                }
            }
        } else {
            quote! {
                if version >= #min {
                    #field_stream
                } else {
                    log::trace!("Field: <{}> is skipped because version: {} is less than min: {}",stringify!(#field_name),version,#min);
                }
            }
        }
    }
}
