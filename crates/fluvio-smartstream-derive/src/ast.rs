use syn::{AttributeArgs, NestedMeta, Meta, ItemFn, Error as SynError, Result as SynResult};
use syn::spanned::Spanned;
use proc_macro2::Ident;

/// The configuration for the Smart Stream that will be generated.
///
/// This is constructed from the AttributeArgs of the derive macro.
#[derive(Debug)]
pub struct SmartStreamConfig {
    pub kind: SmartStreamKind,
}

impl SmartStreamConfig {
    #[allow(clippy::ptr_arg)]
    pub fn from_ast(args: &AttributeArgs) -> SynResult<Self> {
        let kind = SmartStreamKind::from_ast(args)?;
        Ok(Self { kind })
    }
}

#[derive(Debug)]
pub enum SmartStreamKind {
    Aggregate,
    Filter,
    Map,
}

impl SmartStreamKind {
    #[allow(clippy::ptr_arg)]
    fn from_ast(args: &AttributeArgs) -> SynResult<Self> {
        let ss_type =
            args.iter()
                .filter_map(|it| match it {
                    NestedMeta::Meta(Meta::Path(it)) => {
                        it.segments.iter().rev().next().and_then(|it| {
                            match &*it.ident.to_string() {
                                "aggregate" => Some(Self::Aggregate),
                                "filter" => Some(Self::Filter),
                                "map" => Some(Self::Map),
                                _ => None,
                            }
                        })
                    }
                    _ => None,
                })
                .next()
                .ok_or_else(|| SynError::new(args[0].span(), "Missing SmartStream type"))?;

        Ok(ss_type)
    }
}

pub struct SmartStreamFn<'a> {
    pub name: &'a Ident,
    pub func: &'a ItemFn,
}

impl<'a> SmartStreamFn<'a> {
    pub fn from_ast(func: &'a ItemFn) -> SynResult<Self> {
        let name = &func.sig.ident;
        Ok(Self { name, func })
    }
}
