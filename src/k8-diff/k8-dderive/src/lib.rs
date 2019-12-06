extern crate proc_macro;

mod diff;

use proc_macro::TokenStream as TokenStream1;
use syn::DeriveInput;


#[proc_macro_derive(Difference)]
pub fn diff(input: TokenStream1) -> TokenStream1  {

    // Parse the string representation
    let ast: DeriveInput = syn::parse(input).unwrap();

    let expanded = diff::geneate_diff_trait(&ast);
    expanded.into()
}


