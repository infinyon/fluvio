use crate::ast::prop::Prop;
use syn::{Fields, Generics, Ident, ItemStruct};

pub(crate) struct KfStruct {
    pub struct_ident: Ident,
    pub props: Vec<Prop>,
    pub generics: Generics,
}

impl KfStruct {
    pub fn from_ast(item: &ItemStruct) -> syn::Result<Self> {
        let struct_ident = item.ident.clone();
        let props: Vec<Prop> = if let Fields::Named(fields) = &item.fields {
            let mut prp = vec![];
            for field in fields.named.iter() {
                prp.push(Prop::from_ast(field)?);
            }
            prp
        } else {
            vec![]
        };
        let generics = item.generics.clone();

        Ok(KfStruct {
            struct_ident,
            props,
            generics,
        })
    }
}
