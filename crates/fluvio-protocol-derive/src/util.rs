use syn::{punctuated::Punctuated, Attribute, Expr, Lit, LitStr, Meta, MetaNameValue, Token};

pub(crate) fn find_attr(attrs: &[Attribute], name: &str) -> Option<Meta> {
    attrs.iter().find_map(|a| {
        if let Ok(nested) = a.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) {
            for meta in nested {
                if meta.path().is_ident(name) {
                    return Some(meta);
                }
            }
            None
        } else {
            None
        }
    })
}

pub(crate) fn find_name_attribute<'a>(meta: &'a Meta, name: &str) -> Option<MetaNameValue> {
    find_meta(meta, name).map(|meta| match meta {
        Meta::NameValue(name_value) => name_value,
        _ => panic!("should not happen"),
    })
}

pub(crate) fn find_meta<'a>(meta: &'a Meta, name: &str) -> Option<Meta> {
    if let Meta::List(list) = meta {
        for meta in list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .unwrap()
        {
            match &meta {
                Meta::NameValue(meta_name_value) => {
                    if meta_name_value.path.is_ident(name) {
                        return Some(meta.clone());
                    }
                }
                Meta::Path(path) => {
                    if path.is_ident(name) {
                        return Some(meta.clone());
                    }
                }
                Meta::List(_) => {}
            }
        }
    }

    None
}

/// find name value with integer value
pub(crate) fn find_int_name_value(version_meta: &Meta, attr_name: &str) -> Option<u64> {
    if let Some(attr) = find_name_attribute(version_meta, attr_name) {
        if let Expr::Lit(expr_lit) = &attr.value {
            if let Lit::Int(version_val) = &expr_lit.lit {
                //  println!("version value: {}",version_val.value());
                return version_val.base10_parse::<u64>().ok();
            } else {
                unimplemented!()
            }
        } else {
            unimplemented!()
        }
    } else {
        None
    }
}

/// find name value with str value
pub(crate) fn find_string_name_value(version_meta: &Meta, attr_name: &str) -> Option<LitStr> {
    if let Some(attr) = find_name_attribute(version_meta, attr_name) {
        if let Expr::Lit(expr_lit) = &attr.value {
            if let Lit::Str(version_val) = &expr_lit.lit {
                //  println!("version value: {}",version_val.value());
                return Some(version_val.clone());
            } else {
                unimplemented!()
            }
        } else {
            unimplemented!()
        }
    } else {
        None
    }
}
