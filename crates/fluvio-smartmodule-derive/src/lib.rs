use proc_macro::TokenStream;
use syn::{AttributeArgs, DeriveInput, ItemFn, parse_macro_input};
use crate::ast::{SmartModuleConfig, SmartModuleFn, SmartModuleKind};
mod ast;
mod util;
mod generator;

#[proc_macro_attribute]
pub fn smartmodule(args: TokenStream, input: TokenStream) -> TokenStream {
    use crate::generator::generate_smartmodule;

    let args = parse_macro_input!(args as AttributeArgs);
    let func = parse_macro_input!(input as ItemFn);

    let config = match SmartModuleConfig::from_ast(&args) {
        Ok(config) => config,
        Err(e) => return e.into_compile_error().into(),
    };
    let func = match SmartModuleFn::from_ast(&func) {
        Ok(func) => func,
        Err(e) => return e.into_compile_error().into(),
    };
    let output = generate_smartmodule(&config, &func);

    output.into()
}

/// Custom derive for creating an struct that can be used as extra params in smartmodules functions.
/// This assumes the struct implements Default and that all fields implement FromStr.
///
#[proc_macro_derive(SmartOpt)]
pub fn smartopt_derive(input: TokenStream) -> TokenStream {
    use crate::generator::opt::impl_smart_opt;
    let input = syn::parse_macro_input!(input as DeriveInput);

    impl_smart_opt(input).unwrap_or_else(|err| err.into_compile_error().into())
}
