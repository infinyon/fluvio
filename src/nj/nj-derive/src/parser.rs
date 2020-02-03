
use syn::{ Result, Token};
use syn::parse::{Parse, ParseStream};
use syn::ItemImpl;
use syn::ItemFn;

pub enum NodeItem {
    Function(ItemFn),
    Impl(ItemImpl),
}


impl Parse for NodeItem {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        
        if lookahead.peek(Token!(impl)) {
            input.parse().map(NodeItem::Impl)
        } else {
            input.parse().map(NodeItem::Function) 
        }
    }
}
