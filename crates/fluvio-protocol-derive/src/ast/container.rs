use quote::ToTokens;
use syn::{punctuated::Punctuated, Attribute, Expr, Lit, Meta, Result, Token};

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
        // Find all supported container level attributes in one go
        for attribute in attributes {
            if attribute.path().is_ident("varint") {
                cont_attr.varint = true;
            } else if attribute.path().is_ident("fluvio") {
                let nested =
                    attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
                for kf_attr in nested {
                    if let Meta::NameValue(name_value) = kf_attr {
                        if name_value.path.is_ident("api_min_version") {
                            if let Expr::Lit(expr_lit) = name_value.value {
                                if let Lit::Int(lit_int) = expr_lit.lit {
                                    cont_attr.api_min_version = lit_int.base10_parse::<u16>()?;
                                }
                            }
                        } else if name_value.path.is_ident("api_max_version") {
                            if let Expr::Lit(expr_lit) = name_value.value {
                                if let Lit::Int(lit_int) = expr_lit.lit {
                                    cont_attr.api_max_version =
                                        Some(lit_int.base10_parse::<u16>()?);
                                }
                            }
                        } else if name_value.path.is_ident("api_key") {
                            if let Expr::Lit(expr_lit) = name_value.value {
                                if let Lit::Int(lit_int) = expr_lit.lit {
                                    cont_attr.api_key = Some(lit_int.base10_parse::<u8>()?);
                                }
                            }
                        } else if name_value.path.is_ident("response") {
                            if let Expr::Lit(expr_lit) = name_value.value {
                                if let Lit::Str(lit_str) = expr_lit.lit {
                                    cont_attr.response = Some(lit_str.value());
                                }
                            }
                        } else {
                            tracing::warn!(
                                "#[fluvio({})] does nothing on the container.",
                                name_value.to_token_stream().to_string()
                            )
                        }
                    } else if let Meta::Path(path) = kf_attr {
                        if path.is_ident("default") {
                            cont_attr.default = true;
                        } else if path.is_ident("trace") {
                            cont_attr.trace = true;
                        } else if path.is_ident("encode_discriminant") {
                            cont_attr.encode_discriminant = true;
                        } else {
                            tracing::warn!(
                                "#[fluvio({})] does nothing on the container.",
                                path.to_token_stream().to_string()
                            )
                        }
                    }
                }
            } else if attribute.path().is_ident("repr") {
                let nested =
                    attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
                for repr_attr in nested {
                    if let Meta::Path(path) = repr_attr {
                        if let Some(int_type) = path.get_ident() {
                            cont_attr.repr_type_name = Some(int_type.to_string());
                        }
                    }
                }
            }
        }
        Ok(cont_attr)
    }
}
