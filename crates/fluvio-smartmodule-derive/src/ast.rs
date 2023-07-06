use syn::meta::ParseNestedMeta;
use syn::{ItemFn, Error as SynError, Result as SynResult};
use syn::spanned::Spanned;
use proc_macro2::Ident;

/// The configuration for the SmartModule that will be generated.
///
/// This is constructed from the AttributeArgs of the derive macro.
#[derive(Debug, Default)]
pub struct SmartModuleConfig {
    pub kind: Option<SmartModuleKind>,
}

impl SmartModuleConfig {
    pub fn parse(&mut self, meta: ParseNestedMeta) -> SynResult<()> {
        let kind = SmartModuleKind::parse(meta)?;

        if let Some(kind) = kind {
            self.kind = Some(kind);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum SmartModuleKind {
    Init,
    LookBack,
    Aggregate,
    Filter,
    Map,
    ArrayMap,
    FilterMap,
}

impl SmartModuleKind {
    fn parse(meta: ParseNestedMeta) -> SynResult<Option<Self>> {
        let maybee_ss_type = match &*meta
            .path
            .get_ident()
            .ok_or_else(|| SynError::new(meta.path.span(), "Missing SmartModule type"))?
            .to_string()
        {
            "aggregate" => Some(Self::Aggregate),
            "filter" => Some(Self::Filter),
            "map" => Some(Self::Map),
            "array_map" => Some(Self::ArrayMap),
            "filter_map" => Some(Self::FilterMap),
            "init" => Some(Self::Init),
            "look_back" => Some(Self::LookBack),
            _ => None,
        };

        let ss_type = maybee_ss_type
            .ok_or_else(|| SynError::new(meta.path.span(), "Invalid SmartModule type"))?;

        Ok(Some(ss_type))
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
