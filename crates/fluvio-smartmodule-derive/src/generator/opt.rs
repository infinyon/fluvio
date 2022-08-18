use proc_macro::TokenStream;
use syn::{Data, DeriveInput, Ident};
use quote::quote;

pub fn impl_smart_opt(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    // parse out all the field names in the struct as `Ident`s
    let fields = match input.data {
        Data::Struct(st) => st.fields,
        _ => {
            return Err(syn::Error::new_spanned(
                input.ident,
                "SmartOpt derive macro only can be used on structs.",
            ))
        }
    };

    let idents: Vec<&Ident> = fields
        .iter()
        .filter_map(|field| field.ident.as_ref())
        .collect::<Vec<&Ident>>();

    let keys: Vec<String> = idents
        .clone()
        .iter()
        .map(|ident| ident.to_string())
        .collect::<Vec<String>>();

    let ty: Vec<_> = fields
        .iter()
        .map(|field| ty_inner_type(&field.ty, "Option").unwrap_or(&field.ty))
        .collect::<Vec<_>>();

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let gen = quote! {
        impl #impl_generics std::convert::TryFrom<fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams
        > for #name #ty_generics #where_clause {
            type Error =  String;
                fn  try_from(params: fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams) -> ::std::result::Result<Self, Self::Error>{
                    let mut output = #name::default();
                #(
                    if let Some(string_value) = params.get(#keys) {
                            let value = match string_value.parse::<#ty>().ok() {
                                Some(val) => val,
                                None => return Err(Default::default())
                            };
                            output.#idents = value.into();
                    }
                )*
                Ok(output)
            }
        }
    };
    Ok(gen.into())
}

fn ty_inner_type<'a>(ty: &'a syn::Type, wrapper: &'static str) -> Option<&'a syn::Type> {
    if let syn::Type::Path(ref p) = ty {
        if p.path.segments.len() == 1 && p.path.segments[0].ident == wrapper {
            if let syn::PathArguments::AngleBracketed(ref inner_ty) = p.path.segments[0].arguments {
                if inner_ty.args.len() == 1 {
                    let inner_ty = inner_ty.args.first().unwrap();
                    if let syn::GenericArgument::Type(ref t) = inner_ty {
                        return Some(t);
                    }
                }
            }
        }
    }
    None
}
