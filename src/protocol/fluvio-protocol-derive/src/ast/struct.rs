use crate::ast::prop::NamedProp;
use syn::{Fields, Generics, Ident, ItemStruct};

pub(crate) struct FluvioStruct {
    pub struct_ident: Ident,
    pub props: Vec<NamedProp>,
    pub generics: Generics,
}

impl FluvioStruct {
    pub fn from_ast(item: &ItemStruct) -> syn::Result<Self> {
        let struct_ident = item.ident.clone();
        let props: Vec<NamedProp> = if let Fields::Named(fields) = &item.fields {
            let mut prp = vec![];
            for field in fields.named.iter() {
                prp.push(NamedProp::from_ast(field)?);
            }
            prp
        } else {
            vec![]
        };
        let generics = item.generics.clone();

        Ok(FluvioStruct {
            struct_ident,
            props,
            generics,
        })
    }
}
