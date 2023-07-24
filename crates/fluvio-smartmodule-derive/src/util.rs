use proc_macro2::Span;

use crate::SmartModuleKind;

pub(crate) fn generate_ident(kind: &SmartModuleKind) -> syn::Ident {
    syn::Ident::new(&kind.to_string(), Span::call_site())
}
