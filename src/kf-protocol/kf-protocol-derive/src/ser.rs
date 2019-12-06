use quote::quote;
use syn::DeriveInput;
use syn::Ident;
use syn::Fields;
use syn::Attribute;
use syn::Data;
use syn::DataStruct;
use syn::DataEnum;
use syn::Expr;
use syn::LitInt;
use syn::Lit;
use syn::IntSuffix;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use syn::spanned::Spanned;
use syn::UnOp;

use crate::default_int_type;
use super::version::Version;

/// generate implementation for encoding kf protocol
pub fn generate_encode_traits(input: &DeriveInput) -> TokenStream {
    
    let name = &input.ident;

    let encoded_field_tokens = encode_fields_for_writing(&input.data,&input.attrs,name);
    let size_field_tokens = encode_field_sizes(&input.data,&input.attrs,name);
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
 
    quote! {

        impl #impl_generics kf_protocol::Encoder for #name #ty_generics #where_clause {

            fn encode<T>(&self, src: &mut T, version: kf_protocol::Version) -> Result<(),std::io::Error> where T: kf_protocol::bytes::BufMut {
                log::trace!("encoding struct: {} version: {}",stringify!(#name),version);
                #encoded_field_tokens
                Ok(())
            }

            fn write_size(&self, version: kf_protocol::Version) -> usize {
                
                log::trace!("write size for struct: {} version {}",stringify!(#name),version);
                let mut len: usize = 0;
                #size_field_tokens
                len
            }


        }
    }
}

/// generate syntax for encoding
fn encode_fields_for_writing(data: &Data,attrs: &Vec<Attribute>,name: &Ident) -> TokenStream  {

    match *data {
        Data::Struct(ref struct_data) => parse_structf_encoding(name,struct_data),
        Data::Enum(ref enum_data) =>  parse_enum_for_writing(enum_data,attrs,name),
        _ => unimplemented!()
    }

}


fn parse_structf_encoding(struct_name: &Ident,data: &DataStruct) -> TokenStream {

    match data.fields  {
        Fields::Named(ref fields) =>  {

            let recurse = fields.named.iter().map(|f| {
                let fname = &f.ident;           

                if f.attrs.iter().flat_map(Attribute::interpret_meta).find( |meta| meta.name() == "varint").is_some() {
                    quote! {
                        log::trace!("encoding varint struct: <{}> field <{}> => {:?}",stringify!(#struct_name),stringify!(#fname),&self.#fname);
                        let result = self.#fname.encode_varint(src);
                        if result.is_err() {
                            log::error!("error varint encoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                            return result;
                        }
                        
                    }
                } else {
                    
                    let base = quote! {
                        log::trace!("encoding struct: <{}>, field <{}> => {:?}",stringify!(#struct_name),stringify!(#fname),&self.#fname);
                        let result = self.#fname.encode(src,version);
                        if result.is_err() {
                            log::error!("Error Encoding <{}> ==> {}",stringify!(#fname),result.as_ref().unwrap_err());
                            return result;
                        }
                        
                    };

                    if let Some(version) = Version::find_version(&f.attrs) {
                        if let Some(msg) = version.validation_msg() {
                            syn::Error::new(f.span(),msg).to_compile_error()    
                            
                        } else {
                            match fname {
                                Some(field_name) => version.expr(base,field_name),
                                None => base
                            }
                                        
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

fn parse_enum_for_writing(data: &DataEnum, attrs: &Vec<Attribute>,name: &Ident) -> TokenStream  {

    // find repr sentation
    let int_type = default_int_type(attrs);
    let mut variant_expr = vec![];
    for (idx, variant) in data.variants.iter().enumerate() {
        let id = &variant.ident;
        //print!("id: {} => ",id);

         match &variant.fields {
             Fields::Unit => {
              //  print!("unit = ");
                if let Some(expr) = &variant.discriminant {


                    let expr = match &expr.1 {
                        Expr::Lit(lit) => {
                            match &lit.lit {
                                Lit::Int(int_lit) => quote! {
                                    #name::#id => {
                                        let val = #int_lit as #int_type;
                                        val.encode(src,version)?; 
                                    }
                                   
                                },
                                _ => quote! {
                                    compile_error!("unsupported")
                                }
                            }
                        },
                        Expr::Unary(t) => {
                            match t.op {
                                UnOp::Neg(_) => quote!{
                                    #name::#id => {
                                        let val = #t as #int_type;
                                        val.encode(src,version)?; 
                                    }
                                },
                                _ => quote! {
                                    compile_error!("unsupported")
                                }
                            }
                           
                        },
                        _ => quote! {
                            compile_error!("unsupported")
                        }
                    };
                    variant_expr.push(expr);
                     
                } else {                    
                    let idx_val  = LitInt::new(idx as u64, IntSuffix::None,Span::call_site());
                    variant_expr.push(quote! {
                          #name::#id => { 
                            let val = #idx_val as #int_type;
                            val.encode(src,version)?; 
                          },
                    });        
                }
             //  

             },
             Fields::Named(_named_fields) =>  {
                variant_expr.push(quote! {
                    compiler_error!("named fields are not supported");
                });
             },
             Fields::Unnamed(_) => {
                // println!("unamed");
                
                variant_expr.push(quote! {
                    #name::#id(val) => val.encode(src,version)?,
                });
                
             }
         }
    }

    quote! {        
        match self {
            #(#variant_expr)*
        }
    }
}


/// generate syntax for encoding
fn encode_field_sizes(data: &Data,attrs: &Vec<Attribute>,name: &Ident) -> TokenStream  {

    match *data {
        Data::Struct(ref struct_data) => parse_structf_size(name,struct_data),
        Data::Enum(ref enum_data) =>  parse_enum_for_size(enum_data,attrs,name),
        _ => unimplemented!()
    }

}


fn parse_structf_size(struct_name: &Ident,data: &DataStruct) -> TokenStream {

    match data.fields  {
        Fields::Named(ref fields) =>  {

            let recurse = fields.named.iter().map(|f| {
                let fname = &f.ident;
                if f.attrs.iter().flat_map(Attribute::interpret_meta).find( |meta| meta.name() == "varint").is_some() {
                    quote! {
                        let write_size = self.#fname.var_write_size();
                        log::trace!("varint write size: <{}>, field: <{}> is: {}",stringify!(#struct_name),stringify!(#fname),write_size);
                        len = len + write_size;
                    }
                } else {
                    let base = quote! {
                        let write_size = self.#fname.write_size(version);
                        log::trace!("write size: <{}> field: <{}> => {}",stringify!(#struct_name),stringify!(#fname),write_size);
                        len = len + write_size;
                    };

                    if let Some(version) = Version::find_version(&f.attrs) {
                        if let Some(msg) = version.validation_msg() {
                            syn::Error::new(f.span(),msg).to_compile_error()    
                            
                        } else {
                            match fname {
                                Some(field_name) => version.expr(base,field_name),
                                None => base
                            }            
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



fn parse_enum_for_size(data: &DataEnum,attrs: &Vec<Attribute>,name: &Ident) -> TokenStream  {
    
    let int_type = default_int_type(attrs);

    let mut variant_expr = vec![];

    for (_, variant) in data.variants.iter().enumerate() {
         let id = &variant.ident;
       // print!("id: {} => ",id);

         match &variant.fields {
             Fields::Unnamed(_) => {
               //  println!("unamed");
                
                variant_expr.push(quote! {
                    #name::#id(val) => val.write_size(version),
                });
             },
              _ => {},
         }
    }

    if variant_expr.len() > 0 {
        quote! {        
            len = match self {
                #(#variant_expr)*
            };
        }
    } else {
        quote! {
            len = std::mem::size_of::<#int_type>();
        }
    }
   
}