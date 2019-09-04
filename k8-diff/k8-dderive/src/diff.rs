use quote::quote;
use proc_macro2::TokenStream;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;



pub fn geneate_diff_trait(input: &DeriveInput) -> TokenStream {
    let name = &input.ident;
    let decoded_field_tokens = decode_fields(&input.data);

    quote! {

        impl <'a>k8_diff::Changes<'a> for #name {

            fn diff(&self, new: &'a Self) -> k8_diff::Diff {

                let mut s_diff = k8_diff::DiffStruct::new();

                #decoded_field_tokens
                
                if s_diff.no_change() {
                    return k8_diff::Diff::None
                }
        
                k8_diff::Diff::Change(k8_diff::DiffValue::Struct(s_diff))
            }
        }

    }
}

fn decode_fields(data: &Data) -> TokenStream {
    match *data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    let recurse = fields.named.iter().map(|f| {
                        let fname = &f.ident;
                        
                        quote! {
                             // s_diff.insert("replicas".to_owned(), self.replicas.diff(&new.replicas));
                            s_diff.insert(stringify!(#fname).to_owned(), self.#fname.diff(&new.#fname));
                            
                        }
                        
                    });

                    quote! {
                      #(#recurse)*
                    }
                }
                _ => unimplemented!(),
            }
        }
        _ => unimplemented!(),
    }
}


