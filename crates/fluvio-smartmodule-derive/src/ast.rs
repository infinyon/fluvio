use syn::{AttributeArgs, NestedMeta, Meta, ItemFn, Error as SynError, Result as SynResult};
use syn::spanned::Spanned;
use proc_macro2::Ident;

/// The configuration for the SmartModule that will be generated.
///
/// This is constructed from the AttributeArgs of the derive macro.
#[derive(Debug)]
pub struct SmartModuleConfig {
    pub kind: SmartModuleKind,
}

impl SmartModuleConfig {
    #[allow(clippy::ptr_arg)]
    pub fn from_ast(args: &AttributeArgs) -> SynResult<Self> {
        let kind = SmartModuleKind::from_ast(args)?;
        Ok(Self { kind })
    }
}

#[derive(Debug)]
pub enum SmartModuleKind {
    Init,
    Aggregate,
    Filter,
    Map,
    ArrayMap,
    FilterMap,
}

impl SmartModuleKind {
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
                                "array_map" => Some(Self::ArrayMap),
                                "filter_map" => Some(Self::FilterMap),
                                "init" => Some(Self::Init),
                                _ => None,
                            }
                        })
                    }
                    _ => None,
                })
                .next()
                .ok_or_else(|| SynError::new(args[0].span(), "Missing SmartModule type"))?;

        Ok(ss_type)
    }
}

pub struct SmartModuleFn<'a> {
    pub name: &'a Ident,
    pub func: &'a ItemFn,
}

impl<'a> SmartModuleFn<'a> {
    pub fn from_ast(func: &'a ItemFn) -> SynResult<Self> {
        let name = &func.sig.ident;
        Ok(Self { name, func })
    }
}
