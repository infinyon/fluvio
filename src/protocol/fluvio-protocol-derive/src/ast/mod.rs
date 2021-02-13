pub(crate) mod container;
pub(crate) mod r#enum;
pub(crate) mod prop;
pub(crate) mod r#struct;

use syn::parse::{Parse, ParseStream};
use syn::{Attribute, ItemEnum, ItemStruct, Result, Token, Visibility};

use crate::ast::container::ContainerAttributes;
use crate::ast::r#enum::FluvioEnum;
use crate::ast::r#struct::FluvioStruct;

pub(crate) enum DeriveItem {
    Struct(FluvioStruct, ContainerAttributes),
    Enum(FluvioEnum, ContainerAttributes),
}

impl Parse for DeriveItem {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = ContainerAttributes::from_ast(&input.call(Attribute::parse_outer)?)?;
        let _vis = input.parse::<Visibility>()?;

        let lookahead = input.lookahead1();
        if lookahead.peek(Token![struct]) {
            let item_struct: ItemStruct = input.parse()?;
            let kf_struct = FluvioStruct::from_ast(&item_struct)?;
            Ok(DeriveItem::Struct(kf_struct, attrs))
        } else if lookahead.peek(Token![enum]) {
            let item_enum: ItemEnum = input.parse()?;
            let kf_enum = FluvioEnum::from_ast(item_enum)?;
            Ok(DeriveItem::Enum(kf_enum, attrs))
        } else {
            Err(lookahead.error())
        }
    }
}
