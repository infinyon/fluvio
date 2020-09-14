use syn::{Attribute, Lit, LitStr, Meta, MetaNameValue, NestedMeta};

pub(crate) fn find_attr(attrs: &[Attribute], name: &str) -> Option<Meta> {
    attrs.iter().find_map(|a| {
        if let Ok(meta) = a.parse_meta() {
            if meta.path().is_ident(name) {
                Some(meta)
            } else {
                //println!("attr name: {}",meta.name());
                None
            }
        } else {
            //println!("unrecog attribute");
            None
        }
    })
}

pub(crate) fn find_name_attribute<'a>(meta: &'a Meta, name: &str) -> Option<&'a MetaNameValue> {
    find_meta(meta, name).map(|meta| match meta {
        Meta::NameValue(name_value) => name_value,
        _ => panic!("should not happen"),
    })
}

pub(crate) fn find_meta<'a>(meta: &'a Meta, name: &str) -> Option<&'a Meta> {
    if let Meta::List(list) = meta {
        for attr in list.nested.iter() {
            if let NestedMeta::Meta(named_meta) = attr {
                match named_meta {
                    Meta::NameValue(meta_name_value) => {
                        if meta_name_value.path.is_ident(name) {
                            return Some(named_meta);
                        }
                    }
                    Meta::Path(path) => {
                        if path.is_ident(name) {
                            return Some(named_meta);
                        }
                    }
                    Meta::List(_) => {}
                }
            }
        }
    }

    None
}

/// find name value with integer value
pub(crate) fn find_int_name_value<'a>(version_meta: &'a Meta, attr_name: &str) -> Option<u64> {
    if let Some(attr) = find_name_attribute(&version_meta, attr_name) {
        match &attr.lit {
            Lit::Int(version_val) => {
                //  println!("version value: {}",version_val.value());
                version_val.base10_parse::<u64>().ok()
            }
            _ => unimplemented!(),
        }
    } else {
        None
    }
}

/// find name value with str value
pub(crate) fn find_string_name_value<'a>(
    version_meta: &'a Meta,
    attr_name: &str,
) -> Option<LitStr> {
    if let Some(attr) = find_name_attribute(&version_meta, attr_name) {
        match &attr.lit {
            Lit::Str(val) => Some(val.clone()),
            _ => unimplemented!(),
        }
    } else {
        None
    }
}
