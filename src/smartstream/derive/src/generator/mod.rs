use proc_macro2::TokenStream;
use crate::{SmartStreamConfig, SmartStreamFn, SmartStreamKind};

mod filter;
mod map;

pub fn generate_smartstream(config: &SmartStreamConfig, func: &SmartStreamFn) -> TokenStream {
    match config.kind {
        SmartStreamKind::Filter => self::filter::generate_filter_smartstream(func),
        SmartStreamKind::Map => self::map::generate_map_smartstream(func),
    }
}
