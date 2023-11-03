use syn::{
    punctuated::Punctuated, Attribute, Expr, Lit, LitStr, Meta, MetaList, MetaNameValue, Token,
};
pub fn get_lit_int<'a>(attr_name: &'a str, value: &'a Expr) -> syn::Result<&'a syn::LitInt> {
    match &value {
        Expr::Lit(lit_expr) => {
            if let Lit::Int(lit) = &lit_expr.lit {
                Ok(lit)
            } else {
                Err(syn::Error::new_spanned(
                    value,
                    format!("Expected {attr_name} attribute to be an int: `{attr_name} = \"...\"`"),
                ))
            }
        }
        _ => Err(syn::Error::new_spanned(
            value,
            format!("Expected {attr_name} attribute to be a Lit: `{attr_name} = \"...\"`"),
        )),
    }
}
pub fn get_lit_str<'a>(attr_name: &'a str, value: &'a Expr) -> syn::Result<&'a syn::LitStr> {
    match &value {
        Expr::Lit(lit_expr) => {
            if let Lit::Str(lit) = &lit_expr.lit {
                Ok(lit)
            } else {
                Err(syn::Error::new_spanned(
                    value,
                    format!(
                        "Expected {attr_name} attribute to be a string: `{attr_name} = \"...\"`"
                    ),
                ))
            }
        }
        _ => Err(syn::Error::new_spanned(
            value,
            format!("Expected {attr_name} attribute to be a Lit: `{attr_name} = \"...\"`"),
        )),
    }
}

pub(crate) fn find_attr(attrs: &[Attribute], name: &str) -> Option<Meta> {
    attrs.iter().find_map(|a| {
        if a.meta.path().is_ident(name) {
            Some(a.meta.clone())
        } else {
            None
        }
    })
}
pub(crate) fn find_name_value_from_meta_list(
    meta_list: &MetaList,
    attr_name: &str,
) -> Option<MetaNameValue> {
    for item in meta_list
        .parse_args_with(Punctuated::<syn::Meta, Token![,]>::parse_terminated)
        .unwrap_or_else(|err| panic!("Invalid #[new] attribute: {}", err))
    {
        if let Meta::NameValue(kv) = &item {
            if kv.path.is_ident(attr_name) {
                return Some(kv.clone());
            }
        };
    }
    None
}

pub(crate) fn find_name_value_from_meta(meta: &Meta, attr_name: &str) -> Option<MetaNameValue> {
    if let Meta::List(list) = meta {
        return find_name_value_from_meta_list(list, attr_name);
    }
    None
}

/// find name value with str value
pub(crate) fn find_string_name_value(meta: &Meta, attr_name: &str) -> Option<LitStr> {
    let meta_name_value = find_name_value_from_meta(meta, attr_name)?;
    get_lit_str(attr_name, &meta_name_value.value).ok().cloned()
}

pub(crate) fn find_int_name_value(meta: &Meta, attr_name: &str) -> Option<u64> {
    let meta_name_value = find_name_value_from_meta(meta, attr_name)?;
    let value = get_lit_int(attr_name, &meta_name_value.value)
        .ok()
        .cloned()?;
    value.base10_parse::<u64>().ok()
}
