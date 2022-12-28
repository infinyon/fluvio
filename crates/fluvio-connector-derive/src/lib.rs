mod ast;
mod generator;

use ast::{ConnectorDirection, ConnectorFn};
use generator::generate_connector;
use proc_macro::TokenStream;
use syn::{parse_macro_input, AttributeArgs, ItemFn};

#[proc_macro_attribute]
pub fn connector(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);

    let direction = match ConnectorDirection::from_ast(&args) {
        Ok(dir) => dir,
        Err(e) => return e.into_compile_error().into(),
    };

    let func = parse_macro_input!(input as ItemFn);
    let func = match ConnectorFn::from_ast(&func) {
        Ok(func) => func,
        Err(e) => return e.into_compile_error().into(),
    };

    let output = generate_connector(direction, &func);

    output.into()
}
