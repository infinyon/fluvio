use std::fmt::{Display, Formatter};
use syn::meta::ParseNestedMeta;
use syn::{ItemFn, Error as SynError, Result as SynResult, Signature};
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

impl Display for SmartModuleKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            SmartModuleKind::Init => "init",
            SmartModuleKind::LookBack => "look_back",
            SmartModuleKind::Aggregate => "aggregate",
            SmartModuleKind::Filter => "filter",
            SmartModuleKind::Map => "map",
            SmartModuleKind::ArrayMap => "array_map",
            SmartModuleKind::FilterMap => "filter_map",
        };

        write!(f, "{}", string)
    }
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

/// The kind of record that the SmartModule function is using.
#[derive(Debug)]
pub enum RecordKind {
    /// Records used previous to the SmartModule Engine v21
    LegacyRecord,
    /// Records used in the SmartModule Engine v21 and later.
    SmartModuleRecord,
}

impl RecordKind {
    fn parse(sig: &Signature) -> Self {
        let smartmodule_ident = Ident::new("SmartModuleRecord", proc_macro2::Span::call_site());
        let found_sm_record_arg = sig
            .inputs
            .iter()
            .filter_map(|arg| match arg {
                syn::FnArg::Typed(t) => Some(t.ty.as_ref()),
                _ => None,
            })
            .filter_map(|path_type| match path_type {
                syn::Type::Reference(t) => Some(t),
                _ => None,
            })
            .filter_map(|type_ref| match type_ref.elem.as_ref() {
                syn::Type::Path(path) => Some(&path.path),
                _ => None,
            })
            .any(|path| {
                path.segments
                    .iter()
                    .any(|segment| segment.ident.eq(&smartmodule_ident))
            });

        if found_sm_record_arg {
            return Self::SmartModuleRecord;
        }

        Self::LegacyRecord
    }
}

pub struct SmartModuleFn<'a> {
    pub name: &'a Ident,
    pub func: &'a ItemFn,
    pub record_kind: RecordKind,
}

impl<'a> SmartModuleFn<'a> {
    pub fn from_ast(func: &'a ItemFn) -> SynResult<Self> {
        let name = &func.sig.ident;
        let record_kind = RecordKind::parse(&func.sig);

        Ok(Self {
            name,
            func,
            record_kind,
        })
    }
}
