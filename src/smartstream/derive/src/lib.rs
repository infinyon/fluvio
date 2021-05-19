use proc_macro::TokenStream;
use syn::{parse_macro_input, AttributeArgs, ItemFn};
use crate::ast::{SmartStreamConfig, SmartStreamFn, SmartStreamKind};

mod ast;
mod generator;

#[proc_macro_attribute]
pub fn smartstream(args: TokenStream, input: TokenStream) -> TokenStream {
    use crate::generator::generate_smartstream;

    let args = parse_macro_input!(args as AttributeArgs);
    let func = parse_macro_input!(input as ItemFn);

    let config = match SmartStreamConfig::from_ast(&args) {
        Ok(config) => config,
        Err(e) => return e.into_compile_error().into(),
    };
    let func = match SmartStreamFn::from_ast(&func) {
        Ok(func) => func,
        Err(e) => return e.into_compile_error().into(),
    };
    let output = generate_smartstream(&config, &func);

    output.into()
}
