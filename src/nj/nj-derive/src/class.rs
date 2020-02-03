use quote::quote;
use syn::ItemImpl;
use syn::ImplItem;
use syn::ImplItemMethod;
use syn::Ident;
use syn::LitStr;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;
use inflector::Inflector;

use crate::MyTypePath;
use crate::FunctionArgMetadata;
use crate::FunctionAttribute;
use crate::FunctionContext;

pub fn generate_class(impl_item: ItemImpl) -> TokenStream {

    //println!("class: {:#?}",impl_item);
    let class_metadata = ClassMetadata::new(impl_item);
    class_metadata.as_token_stream()
}

struct ClassMetadata {
    item: ItemImpl,
}

impl ClassMetadata {

    fn new(item: ItemImpl) -> Self {
        Self {
            item
        }
    }

    /// extract class type
    fn class_type(&self) -> Option<MyTypePath> {
        MyTypePath::from(self.item.self_ty.clone())
    }


    /// find methods which are defined in node_bindgen annotation
    fn generate_properties(&self) -> (Vec<TokenStream>,Option<ImplItemMethod>) {

        let mut constructor: Option<ImplItemMethod> = None;
        let properties = self.item.items.iter()
            .filter_map(|i_item| {
                match i_item {
                    ImplItem::Method(method) => {
                        if is_js_method(method) {
                            let method_ident = &method.sig.ident;
                            let property_name = LitStr::new(&method_ident.to_string().to_camel_case(),Span::call_site());
                            let napi_name = Ident::new(&format!("napi_{}",method_ident),Span::call_site());

                            let attribute = FunctionTags::parse_attr(method);
                            if attribute.is_getter() {
                                Some(quote! {
                                    nj::core::Property::new(#property_name).getter(Self::#napi_name),
                                })
                            } else if attribute.is_constructor() {
                                match constructor {
                                    Some(_) => {},
                                    None => {
                                        std::mem::replace(&mut constructor,Some(method.clone()));
                                    }
                                }
                                None
                            } else {                               
                                Some(quote! {
                                    nj::core::Property::new(#property_name).method(Self::#napi_name),
                                })
                            }
                        } else {
                            None
                        }
                    },
                    _ => None
                }
            })
            .collect();

        (properties,constructor)
    }

    /// create constructor method
    /// rust method must return Self
    fn class_constructor(&self,method_opt: Option<ImplItemMethod>) -> (TokenStream,Option<FunctionArgMetadata>) {

        let (arg_conversion, metadata) = match method_opt {

            Some(method) => {
                let method_ident = &method.sig.ident;
                let arg_metadata = match FunctionArgMetadata::parse(&method.sig) {
                    Ok(arg) => arg,
                    Err(err) => {
                        eprintln!("error parsing sig: {}",err);
                        return (quote! {
                            compile_error!("unsupported argument types")
                        }, None)
                    }
                };

                let mut ctx = FunctionContext::default();
                let arg_tokens = arg_metadata.as_arg_token(&ctx);
                let rust_inputs = arg_metadata.rust_args_input(&mut ctx);

                (quote! {

                    #arg_tokens

                    let rust_value = Self::#method_ident(  #(#rust_inputs)* );
                    Ok((rust_value,js_cb))
                    
                }, Some(arg_metadata))
            },
            None => (quote! {
                let js_cb = js_env.get_cb_info(cb_info,0)?;
                Ok((Self::new(),js_cb))
            }, None)
        };
        
        (quote! {
            fn create_from_js(js_env: &nj::core::val::JsEnv, cb_info: nj::sys::napi_callback_info) ->  Result<(Self,nj::core::val::JsCallback),nj::core::NjError> {

                #arg_conversion
            }
        },metadata)
    }

    /// generate class constructor
    fn generate_class_arg(&self,class_name: &Ident,arg_option: Option<FunctionArgMetadata> ) -> TokenStream {

        if let Some(arg_metadata) = arg_option {

            let args = arg_metadata.constructor_args();
            let struct_args = arg_metadata.constructor_new();
            let constr_conversion = arg_metadata.as_constructor_try_to_js();
            let invocation = arg_metadata.as_constructor_invocation();
            let construct_name = Ident::new(&format!("{}Constructor",class_name),Span::call_site());
            quote! {

                pub struct #construct_name {
                    #args
                }

                impl #construct_name {
                    pub fn new(#args) -> Self {
                        Self {
                            #struct_args
                        }
                    }
                }

                impl nj::core::TryIntoJs for #construct_name {

                    fn try_to_js(self, js_env: &nj::core::val::JsEnv) -> Result<nj::sys::napi_value,nj::core::NjError> {

                        #constr_conversion
                        let new_instance = #class_name::new_instance(js_env,vec![#invocation])?;
                        Ok(new_instance)
                    }
                }
            }
        } else {
            quote! {}
        }

    }

    // generate internal module that contains Js class helper
    fn generate_class_helper(&self,class_type: &MyTypePath) -> TokenStream {

        let type_name = class_type.type_name().unwrap();

        let helper_module_name = Ident::new(
            &format!("{}_helper",type_name).to_lowercase(),
            Span::call_site());

        let class_type_lit = LitStr::new(&format!("{}",type_name),Span::call_site());

        let ty = class_type.inner();

        let (properties,constructor ) = self.generate_properties();

        let ( constructor_token,fn_arg) = self.class_constructor(constructor);

        let constructor_arg = self.generate_class_arg(&type_name,fn_arg);

        let construct_name = Ident::new(&format!("{}Constructor",type_name),Span::call_site());

        quote!{

            use #helper_module_name::#construct_name;

            mod #helper_module_name {

                use std::ptr;
                use nj::core::JSClass;

                use super::#ty;

                static mut CLASS_CONSTRUCTOR: nj::sys::napi_ref = ptr::null_mut();

                impl nj::core::JSClass for #ty {
                    const CLASS_NAME: &'static str = #class_type_lit;
                  

                    fn set_constructor(constructor: nj::sys::napi_ref) {
                        unsafe {
                            CLASS_CONSTRUCTOR = constructor;
                        }
                    }
            
                    fn get_constructor() -> nj::sys::napi_ref {
                        unsafe { CLASS_CONSTRUCTOR }
                    }

                    fn properties() -> nj::core::PropertiesBuilder {
            
                        vec![
                            #(#properties)*
                        ].into()
                    }

                    #constructor_token
            
                }

                #constructor_arg

                use nj::core::submit_register_callback;


                #[nj::core::ctor]
                fn register_class() {
                    submit_register_callback(#ty::js_init);
                }
            }


        }
    }

    fn as_token_stream(&self) -> TokenStream {

        let class_type = match self.class_type() {
            Some(info) => info,
            None => return quote! {
                compile_error!("can only handle type path for now")
            }
        };

        let item = &self.item;
        let class_helper = self.generate_class_helper(&class_type);

        quote! {

            #item

            #class_helper

        }
    }
}


fn is_js_method(method: &ImplItemMethod) -> bool {

    method.attrs.iter()
        .find(|attr| {
            attr.path.segments.iter().find( |seg| seg.ident == "node_bindgen").is_some()
        }).is_some()

}

struct FunctionTags(Vec<FunctionAttribute>);

impl FunctionTags {

    fn parse_attr(method: &ImplItemMethod) -> FunctionTags {

        let mut tags = vec![];
        for attr in method.attrs.iter() {
        
            for group_token in attr.tokens.clone().into_iter() {
            
                match group_token {

                    TokenTree::Group(group) => {
                        for iden_token in group.stream().into_iter() {

                            match iden_token {
                                TokenTree::Ident(ident) => {
                                    if ident == "getter" {
                                        tags.push(FunctionAttribute::Getter);
                                    } else if ident == "constructor" {
                                        tags.push(FunctionAttribute::Constructor);
                                    } else {
                                        tags.push(FunctionAttribute::Other);
                                    }
                                },
                                _ => {
                                    tags.push(FunctionAttribute::Other);
                                }
                            }
                            
                        }
                    
                    },
                    _ => { 
                        tags.push(FunctionAttribute::Other);
                    }

                    
                }
            }
        }

        Self(tags)
    }


    fn is_getter(&self) -> bool {
        self.0.iter().find(|tag| {
            match tag {
               FunctionAttribute::Getter => true,
               _ => false 
            }
        }).is_some()
    }

    fn is_constructor(&self) -> bool {
        self.0.iter().find(|tag| {
            match tag {
               FunctionAttribute::Constructor => true,
               _ => false 
            }
        }).is_some()
    }



}



