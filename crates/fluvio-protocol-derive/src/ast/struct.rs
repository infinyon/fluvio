use crate::ast::prop::{NamedProp, UnnamedProp};
use syn::{Fields, Generics, Ident, ItemStruct};

pub(crate) enum FluvioStruct {
    Named(FluvioNamedStruct),
    Tuple(FluvioTupleStruct),
}

pub(crate) struct FluvioNamedStruct {
    pub struct_ident: Ident,
    pub props: Vec<NamedProp>,
    generics: Generics,
}

impl FluvioStruct {
    pub fn from_ast(item: &ItemStruct) -> syn::Result<Self> {
        let struct_ident = item.ident.clone();
        let generics = item.generics.clone();

        let fluvio_struct = match &item.fields {
            Fields::Named(fields) => {
                let mut props = vec![];
                for field in fields.named.iter() {
                    props.push(NamedProp::from_ast(field)?);
                }

                FluvioStruct::Named(FluvioNamedStruct {
                    struct_ident,
                    props,
                    generics,
                })
            }
            Fields::Unnamed(fields) => {
                let mut props = vec![];
                for field in fields.unnamed.iter() {
                    props.push(UnnamedProp::from_ast(field)?);
                }
                FluvioStruct::Tuple(FluvioTupleStruct {
                    struct_ident,
                    props,
                    generics,
                })
            }

            Fields::Unit => FluvioStruct::Tuple(FluvioTupleStruct {
                struct_ident,
                props: vec![],
                generics,
            }),
        };

        Ok(fluvio_struct)
    }

    pub fn struct_ident(&self) -> &Ident {
        match self {
            FluvioStruct::Named(inner) => &inner.struct_ident,
            FluvioStruct::Tuple(inner) => &inner.struct_ident,
        }
    }

    pub fn generics(&self) -> &Generics {
        match self {
            FluvioStruct::Named(inner) => &inner.generics,
            FluvioStruct::Tuple(inner) => &inner.generics,
        }
    }

    pub fn props(&self) -> FluvioStructProps {
        match self {
            FluvioStruct::Named(inner) => FluvioStructProps::Named(inner.props.clone()),
            FluvioStruct::Tuple(inner) => FluvioStructProps::Unnamed(inner.props.clone()),
        }
    }
}

pub(crate) enum FluvioStructProps {
    Named(Vec<NamedProp>),
    Unnamed(Vec<UnnamedProp>),
}

pub(crate) struct FluvioTupleStruct {
    pub struct_ident: Ident,
    pub props: Vec<UnnamedProp>,
    pub generics: Generics,
}
