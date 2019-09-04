use quote::quote;
use proc_macro2::TokenStream;
use proc_macro2::Span;
use syn::Attribute;
use syn::Data;
use syn::DataStruct;
use syn::DataEnum;
use syn::DeriveInput;
use syn::Fields;
use syn::Ident;
use syn::LitInt;
use syn::IntSuffix;
use syn::Expr;
use syn::Lit;
use syn::UnOp;


use crate::default_int_type;
use super::version::Version;
use super::util::find_attr;
use super::util::find_string_name_value;

/// generate implementation for decoding kf protocol
pub fn generate_decode_traits(input: &DeriveInput) -> TokenStream {

    let name = &input.ident;

    let int_type = default_int_type(&input.attrs);

    let decoded_field_tokens = decode_fields(&input.data,&int_type,name);
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let try_enum = generate_try_enum_if(&input.data,&int_type,name);

    quote! {

        impl #impl_generics kf_protocol::Decoder for #name #ty_generics #where_clause {
            fn decode<T>(&mut self, src: &mut T,version: kf_protocol::Version) -> Result<(),std::io::Error> where T: kf_protocol::bytes::Buf {
                log::trace!("decoding struct: {}",stringify!(#name));
                #decoded_field_tokens
                Ok(())
            }

        }

        #try_enum
            
    }
}

/// generate syntax for decoding
fn decode_fields(data: &Data,int_type: &Ident,name: &Ident) -> TokenStream {
    match *data {
        Data::Struct(ref data) => parse_struct(name,data),
        Data::Enum(ref enum_data) => parse_enum(enum_data,int_type,name),
         _ => unimplemented!()
    }
}


fn parse_struct(struct_name: &Ident,data: &DataStruct) -> TokenStream {
   
    match data.fields {
        Fields::Named(ref fields) => {
            let recurse = fields.named.iter().map(|f| {
                let fname = &f.ident;
                if f.attrs
                    .iter()
                    .flat_map(Attribute::interpret_meta)
                    .find(|meta| meta.name() == "varint")
                    .is_some()
                {
                    quote! {
                      
                        log::trace!("start decoding varint field <{}>",stringify!(#fname));
                        let result = self.#fname.decode_varint(src);
                        if result.is_ok() {
                            log::trace!("decoding ok varint <{}> => {:?}",stringify!(#fname),&self.#fname);
                        } else {
                            log::trace!("decoding varint error <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                            return result;
                        }

                    }
                } else {

                    let base = quote! {
            
                        log::trace!("start decoding struct: <{}> field: <{}>",stringify!(#struct_name),stringify!(#fname));
                        let result = self.#fname.decode(src,version);
                        if result.is_ok() {
                            log::trace!("decoding struct: <{}> field: <{}> => {:#?}",stringify!(#struct_name),stringify!(#fname),&self.#fname);
                        } else {
                            log::trace!("error decoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                            return result;
                        }
                    };

                     if let Some(version) = Version::find_version(&f.attrs) {
                        match fname {
                            Some(field_name) => version.expr(base,field_name),
                            None => base
                        }
                    } else {
                        base
                    }  

                }
            });

            quote! {
                #(#recurse)*
            }
        },
         _ => unimplemented!()
    }
            
}


fn parse_enum(_data: &DataEnum,int_type: &Ident,_name: &Ident) -> TokenStream  {
    
    quote! {
        use std::convert::TryInto;

        let mut value: #int_type = 0;
        value.decode(src,version)?;

        let convert: Self = value.try_into()?;
        *self = convert;
    }
}


/// generate syntax for decoding
fn generate_try_enum_if(data: &Data,int_type: &Ident,name: &Ident) -> TokenStream {
    match *data {
        Data::Struct(ref _data) => quote! {},
        Data::Enum(ref enum_data) => generate_try_enum(enum_data,int_type,name),
         _ => unimplemented!()
    }
}

fn generate_try_enum(data: &DataEnum,int_type: &Ident,name: &Ident) -> TokenStream {

    
    let mut variant_expr = vec![];

    for (idx, variant) in data.variants.iter().enumerate() {
         let id = &variant.ident;
      
         match &variant.fields {
             Fields::Unit => {
                
                if let Some(expr) = &variant.discriminant {
                  
                    let int_expr_result = match &expr.1 {

                        Expr::Lit(lit) => {
                            

                            match &lit.lit {
                                Lit::Int(int_lit) => quote! {
                                    #int_lit =>  Ok(#name::#id),
                                },
                                _ => quote! {
                                    compile_error!("unsupported")
                                }
                            }
                          
                        },
                        Expr::Unary(t) => {
                            match t.op {
                                UnOp::Neg(_) => {
                                    
                                    quote! {
                                        #t =>  Ok(#name::#id),
                                    } 
                                },
                                _ => quote! {
                                    compile_error!("unsupported")
                                }
                            }
                           
                        },

                        _ => {
                            quote! {
                                    compile_error!("unsupported")
                            }
                        }
                    };

                    
                    variant_expr.push(int_expr_result);
                  

                } else {
                    
                  

                    let idx_val  = LitInt::new(idx as u64, IntSuffix::None,Span::call_site());
                   
                    variant_expr.push(quote! {
                        #idx_val =>   Ok(#name::#id),
                    });        
                }
             //  

             },
             Fields::Named(_named_fields) =>  {
                variant_expr.push(quote! {
                    compiler_error!("name fields are not supported");
                });
             },
             Fields::Unnamed(_unamed) => {
                 variant_expr.push(quote! {
                    compiler_error!("unnamed fields are not supported");
                });
             }
         }
    }

    
    variant_expr.push(quote! {
        _ => return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("invalid value: {}",value)
                ))
    });

    quote! {
        impl std::convert::TryFrom<#int_type> for #name {
            type Error = std::io::Error;

            fn try_from(value: #int_type) -> Result<Self, Self::Error> {

                match value  {

                    #(#variant_expr)*
                }

            }
        }
    }
      

}




/// generate implementation for decoding kf protocol
pub fn generate_default_traits(input: &DeriveInput) -> TokenStream {

    let name = &input.ident;

    let default_impl = generate_default_impl(input,name);


    quote! {

        #default_impl
            
    }
}


/// generate syntax for decoding
fn generate_default_impl(input: &DeriveInput,name: &Ident) -> TokenStream {

    let data = &input.data;
    match *data {
        Data::Struct(ref data) => impl_default_impl(input,data,name),
        Data::Enum( _) => quote! {},
         _ => unimplemented!()
    }
}


// generates parts of the impl
// 
//  impl Default for TestRequest {
//      fn default() -> Self {
//             
//          Self {
//              field: 10,
//              field2: 20,
//
//
//      }
//
fn impl_default_impl(input: &DeriveInput, data: &DataStruct,name: &Ident) -> TokenStream {


    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
 
    match data.fields {
        Fields::Named(ref fields) => {
            let recurse = fields.named.iter().map(|f| {
                let fname = &f.ident;
               
            
                if let Some(default_attr) = find_attr(&f.attrs,"fluvio_kf") {

                    if let Some(expr_str) = find_string_name_value(&default_attr, "default") {
 
                      
                        
                        use std::str::FromStr;
                        use syn::spanned::Spanned;


                        match TokenStream::from_str(&expr_str.value()) {
                            Err(_err) => syn::Error::new(f.span(),"can't parse default value").to_compile_error(),
                            Ok(liter) => {
                                quote! {
                                    #fname: #liter,
                                }
                            }
                        }
                        
                        
                    } else {
                        quote!{
                            #fname: std::default::Default::default(),
                        }
                    }
                } else {
                    quote!{
                            #fname: std::default::Default::default(),
                    }
                }
                
            });

            quote! {

                impl #impl_generics Default for #name #ty_generics #where_clause {

                    fn default() -> Self {
                        Self {
                            #(#recurse)*
                        }
                    }
                }
               
            }
        },
         _ => unimplemented!()
    }
            
}