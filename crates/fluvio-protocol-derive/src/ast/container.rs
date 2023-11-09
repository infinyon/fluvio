use quote::ToTokens;
use syn::{punctuated::Punctuated, Attribute, Meta, Result, Token};

use crate::util::{get_lit_int, get_lit_str};

#[derive(Debug, Default)]
pub struct ContainerAttributes {
    pub varint: bool,
    pub default: bool,

    pub encode_discriminant: bool,
    pub api_min_version: u16,
    pub api_max_version: Option<u16>,
    pub api_key: Option<u8>,
    pub response: Option<String>,
    pub repr_type_name: Option<String>,
    pub trace: bool,
}

impl ContainerAttributes {
    pub fn from_ast(attributes: &[Attribute]) -> Result<ContainerAttributes> {
        let mut cont_attr = ContainerAttributes::default();

        for attr in attributes {
            if let Some(ident) = attr.path().get_ident() {
                if ident == "varint" {
                    cont_attr.varint = true;
                } else if ident == "fluvio" {
                    if let Meta::List(list) = &attr.meta {
                        if let Ok(list_args) =
                            list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                        {
                            for args_meta in list_args.iter() {
                                if let Meta::NameValue(args_data) = args_meta {
                                    let lit_expr = &args_data.value;
                                    if let Some(args_name) = args_data.path.get_ident() {
                                        if args_name == "api_min_version" {
                                            let value = get_lit_int("api_min_version", lit_expr)?;
                                            cont_attr.api_min_version =
                                                value.base10_parse::<u16>()?;
                                        } else if args_name == "api_max_version" {
                                            let value = get_lit_int("api_max_version", lit_expr)?;
                                            cont_attr.api_max_version =
                                                Some(value.base10_parse::<u16>()?);
                                        } else if args_name == "api_key" {
                                            let value = get_lit_int("api_key", lit_expr)?;
                                            cont_attr.api_key = Some(value.base10_parse::<u8>()?);
                                        } else if args_name == "response" {
                                            let value = get_lit_str("response", lit_expr)?;
                                            cont_attr.response = Some(value.value());
                                        } else {
                                            tracing::warn!(
                                                "#[fluvio({})] does nothing on the container.",
                                                args_data.to_token_stream().to_string()
                                            )
                                        }
                                    }
                                } else if let Meta::Path(path) = args_meta {
                                    if let Some(nested_ident) = path.get_ident() {
                                        if nested_ident == "default" {
                                            cont_attr.default = true;
                                        } else if nested_ident == "trace" {
                                            cont_attr.trace = true;
                                        } else if nested_ident == "encode_discriminant" {
                                            cont_attr.encode_discriminant = true;
                                        } else {
                                            tracing::warn!(
                                                "#[fluvio({})] does nothing on the container.",
                                                nested_ident.to_token_stream().to_string()
                                            )
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if ident == "repr" {
                    if let Meta::List(list) = &attr.meta {
                        if let Ok(list_args) =
                            list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                        {
                            for args_meta in list_args.iter() {
                                if let Meta::Path(path) = args_meta {
                                    if let Some(int_type) = path.get_ident() {
                                        cont_attr.repr_type_name = Some(int_type.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(cont_attr)
    }
}
