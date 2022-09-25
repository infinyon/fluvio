use proc_macro2::Span;

pub(crate) fn ident(ident: &str) -> syn::Ident {
    syn::Ident::new(ident, Span::call_site())
}
