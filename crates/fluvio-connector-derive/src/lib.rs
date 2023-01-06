mod ast;
mod generator;

use ast::{ConnectorDirection, ConnectorFn, ConnectorConfigStruct};
use generator::{generate_connector, generate_connector_config};
use proc_macro::TokenStream;
use syn::{parse_macro_input, AttributeArgs, ItemFn, Item, ItemStruct, Error, spanned::Spanned};

#[proc_macro_attribute]
pub fn connector(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as AttributeArgs);
    let input = parse_macro_input!(input as Item);

    match input {
        Item::Fn(item_fn) => connector_func(args, item_fn),
        Item::Struct(item_struct) => connector_config(args, item_struct),
        _ => Error::new(input.span(), "macro supports only functions and structs")
            .into_compile_error()
            .into(),
    }
}

fn connector_func(args: AttributeArgs, func: ItemFn) -> TokenStream {
    let direction = match ConnectorDirection::from_ast(&args) {
        Ok(dir) => dir,
        Err(e) => return e.into_compile_error().into(),
    };

    let func = match ConnectorFn::from_ast(&func) {
        Ok(func) => func,
        Err(e) => return e.into_compile_error().into(),
    };

    let output = generate_connector(direction, &func);

    output.into()
}

fn connector_config(args: AttributeArgs, item: ItemStruct) -> TokenStream {
    let item = match ConnectorConfigStruct::from_ast(&args, &item) {
        Ok(it) => it,
        Err(e) => return e.into_compile_error().into(),
    };
    let output = generate_connector_config(&item);
    output.into()
}
