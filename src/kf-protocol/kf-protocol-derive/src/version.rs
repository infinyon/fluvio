

use syn::Attribute;
use syn::Ident;
use quote::quote;
use proc_macro2::TokenStream;

use super::util::find_attr;
use super::util::find_int_name_value;


pub(crate) struct Version {
    min: i16,
    max: Option<i16>
}

impl Version {
    
    // find fluvio versions
    pub(crate) fn find_version(attrs: &Vec<Attribute>) -> Option<Self> {
        
        if let Some(version) = find_attr(attrs,"fluvio_kf") {

            if let Some(min) = find_int_name_value(&version, "min_version") {

                let max = find_int_name_value(&version,"max_version"); 
                Some(
                    Self {
                        min: min as i16,
                        max: max.map(|v| v as i16)
                    }
                )
            } else {
                None
            }                 
        } else {
            None
        }               
    }

    pub(crate) fn validation_msg(&self) -> Option<String> {
        if let Some(max) = self.max {
            if self.min > max {
                Some("max version is less then min".to_owned())
            } else {
                None
            }
        } else {
            if self.min < 0 {
                Some("min version must be positive".to_owned())
            } else {
                None
            }
        }
    }

    // generate expression
    pub(crate) fn expr(&self,input: TokenStream,field_name: &Ident) -> TokenStream {

        let min = self.min;


        if let Some(max) = self.max {
            quote! {
                if version >= #min && version <= #max {
                    #input
                } else {
                    log::trace!("field: <{}> is skipped because version: {} is outside min: {}, max: {}",stringify!(#field_name),version,#min,#max);
                }
            }
        } else {
            quote! {
                if version >= #min {
                    #input
                } else {
                    log::trace!("field: <{}> is skipped because version: {} is less than min: {}",stringify!(#field_name),version,#min);
                }
            }
        }

    }
}
