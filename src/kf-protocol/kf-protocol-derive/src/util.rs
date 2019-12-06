
use syn::Ident;
use syn::Attribute;
use syn::Meta;
use syn::NestedMeta;
use syn::MetaNameValue;
use syn::Lit;
use syn::LitStr;
use proc_macro2::Span;


/// find type using rep, if not found return u8
pub(crate) fn default_int_type(attrs: &Vec<Attribute>) -> Ident {
    let mut rep_list = vec![];
    for attr in attrs {
        let meta = attr.parse_meta().expect("meta");
        match meta {
            Meta::List(meta_list) => {
                for attr_meta in meta_list.nested.iter() {
                    match attr_meta {
                        NestedMeta::Meta(inner_meta) => {                            
                            match inner_meta {
                                Meta::Word(ident) => rep_list.push(ident.clone()),
                                _ => {}
                            } 
                        }
                        _ => {}
                    }
                   
                }
            }
            _ => {}
        }
       
    }
    
    if rep_list.len() == 0 { Ident::new("u8",Span::call_site()) } else { rep_list.remove(0) }

}



pub(crate) fn find_attr(attrs: &Vec<Attribute>,name: &str) -> Option<Meta> {
    attrs.iter()
        .find_map(|a| {
            if let Ok(meta) = a.parse_meta() {
                if meta.name() == name {
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

pub(crate) fn find_name_attribute<'a>(meta: &'a Meta,name: &str) -> Option<&'a MetaNameValue> {
    find_meta(meta,name).map(|meta| match meta {
        Meta::NameValue(name_value) => name_value,
        _ => panic!("should not happen")
    })
}


pub(crate) fn find_meta<'a>(meta: &'a Meta,name: &str) -> Option<&'a Meta> {
   
    match meta {
        Meta::List(list) => {
            for attr in list.nested.iter() {
                match attr {
                    NestedMeta::Meta(named_meta) => {
                        
                        match named_meta {
                            Meta::NameValue(name_value) => {
                                if name_value.ident == name {
                                    return Some(named_meta)
                                }
                                
                            }
                            Meta::Word(word_value) => {
                                if word_value == name {
                                    return Some(named_meta)
                                }
                                {}
                            },
                            Meta::List(_) => {
                                {}
                            }
                        }

                    },
                    _ => {}
                }
                
            }

            {}
        },
        _ => {}
    }

    None
        
}


/// find name value with integer value
pub(crate) fn find_int_name_value<'a>(version_meta: &'a Meta,attr_name: &str) -> Option<u64> {
                   
    if let Some(attr) = find_name_attribute(&version_meta,attr_name) {
        
        match &attr.lit {
            Lit::Int(version_val) => { 
              //  println!("version value: {}",version_val.value());
                Some(version_val.value())
            },
            _ => unimplemented!()
        }
    } else {
        None
    }

}


/// find name value with str value
pub(crate) fn find_string_name_value<'a>(version_meta: &'a Meta,attr_name: &str) -> Option<LitStr> {
                   
    if let Some(attr) = find_name_attribute(&version_meta,attr_name) {
        
        match &attr.lit {
            Lit::Str(val) => { 
                Some(val.clone())
            },
            _ => unimplemented!()
        }
    } else {
        None
    }

}

