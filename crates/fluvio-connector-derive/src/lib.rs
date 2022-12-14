mod ast;
mod generator;

use ast::{ConnectorDirection, ConnectorFn};
use generator::generate_connector;
use proc_macro::TokenStream;
use syn::{spanned::Spanned, parse_macro_input, AttributeArgs, Error, ItemFn};

#[proc_macro_attribute]
pub fn connector(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);

    let direction = match ConnectorDirection::from_ast(&args) {
        Ok(ConnectorDirection::Source) => ConnectorDirection::Source,
        Ok(ConnectorDirection::Sink) => {
            return Error::new(args[0].span(), "Sink Connectors are not implemented yet")
                .to_compile_error()
                .into()
        }
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
