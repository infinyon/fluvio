pub(crate) mod container;
pub(crate) mod r#enum;
pub(crate) mod prop;
pub(crate) mod r#struct;

use syn::parse::{Parse, ParseStream};
use syn::{Attribute, ItemEnum, ItemStruct, Result, Token, Visibility};

use crate::ast::container::ContainerAttributes;
use crate::ast::r#enum::KfEnum;
use crate::ast::r#struct::KfStruct;

pub(crate) enum DeriveItem {
    Struct(KfStruct, ContainerAttributes),
    Enum(KfEnum, ContainerAttributes),
}

impl Parse for DeriveItem {
    fn parse(input: ParseStream) -> Result<Self> {
        let attrs = ContainerAttributes::from_ast(&input.call(Attribute::parse_outer)?)?;
        let _vis = input.parse::<Visibility>()?;

        let lookahead = input.lookahead1();
        if lookahead.peek(Token![struct]) {
            let item_struct: ItemStruct = input.parse()?;
            let kf_struct = KfStruct::from_ast(&item_struct)?;
            Ok(DeriveItem::Struct(kf_struct, attrs))
        } else if lookahead.peek(Token![enum]) {
            let item_enum: ItemEnum = input.parse()?;
            let kf_enum = KfEnum::from_ast(item_enum)?;
            Ok(DeriveItem::Enum(kf_enum, attrs))
        } else {
            Err(lookahead.error())
        }
    }
}
