use syn::{Attribute, Expr, Lit, LitStr, Meta, punctuated::Punctuated, Token, MetaNameValue, MetaList};
pub fn get_lit_int(attr_name: String, value: &Expr) -> syn::Result<&syn::LitInt> {
    match &value {
        Expr::Lit(lit_expr) => {
            if let Lit::Int(lit) = &lit_expr.lit {
                Ok(lit)
             } else {
                 Err(syn::Error::new_spanned(
                     value,
                     format!("Expected {attr_name} attribute to be a int: `{attr_name} = \"...\"`"),
                 ))
             }
        },
        _ => {
            Err(syn::Error::new_spanned(
                value,
                format!("Expected {attr_name} attribute to be Path or Lit: `{attr_name} = \"...\"`"),
            ))
        },
    }
}
pub fn get_lit_str(attr_name: String, value: &Expr) -> syn::Result<&syn::LitStr> {
    match &value {
        Expr::Lit(lit_expr) => {
            if let Lit::Str(lit) = &lit_expr.lit {
                Ok(lit)
             } else {
                 Err(syn::Error::new_spanned(
                     value,
                     format!("Expected {attr_name} attribute to be a string: `{attr_name} = \"...\"`"),
                 ))
             }
        },
        _ => {
            Err(syn::Error::new_spanned(
                value,
                format!("Expected {attr_name} attribute to be Path or string: `{attr_name} = \"...\"`"),
            ))
        },
    }
}

pub(crate) fn find_attr(attrs: &[Attribute], name: &str) -> Option<Meta> {
    attrs.iter().find_map(|a| {
        if a.meta.path().is_ident(name) {
            Some(a.meta.clone())
        } else {
            //println!("attr name: {}",meta.name());
            None
        }
    })
}
pub(crate) fn find_name_value_from_meta_list(meta_list: &MetaList, attr_name: &str) -> Option<MetaNameValue> {
    let mut result_meta: Option<MetaNameValue> = None;
    for item in meta_list
        .parse_args_with(Punctuated::<syn::Meta, Token![,]>::parse_terminated)
        .unwrap_or_else(|err| panic!("Invalid #[new] attribute: {}", err))
    {
        match &item {
            Meta::NameValue(kv) => {
                if kv.path.is_ident(attr_name) {
                    result_meta = Some(kv.clone());
                    break;
                }
            }
            _ => {  }
        };

    }

    result_meta
}

pub(crate) fn find_name_value_from_meta(meta: &Meta, attr_name: &str) -> Option<MetaNameValue> {
    let mut result_meta: Option<MetaNameValue> = None;
    if let Meta::List(list) = meta {
        result_meta = find_name_value_from_meta_list(list, attr_name);
    }

    result_meta
}

/// find name value with str value
pub(crate) fn find_string_name_value(meta: &Meta, attr_name: &str) -> Option<LitStr> {
    let mut value: Option<LitStr> = None;
    if let Some(meta_name_value) = find_name_value_from_meta(meta, attr_name) {
        let data = get_lit_str(String::from(attr_name), &meta_name_value.value);
        value = match data {
            Ok(val) => {
                Some(val.clone())
            },
            Err(_) => None,
        }
    }
    
    value
}

pub(crate) fn find_int_name_value(meta: &Meta, attr_name: &str) -> Option<u64> {
    let mut value: Option<u64> = None;
    if let Some(meta_name_value) = find_name_value_from_meta(meta, attr_name) {

        let data = get_lit_int(String::from(attr_name), &meta_name_value.value);
        value = match data {
            Ok(val) => {
                val.base10_parse::<u64>().ok()
            },
            Err(_) => None,
        }
    }

    value
}