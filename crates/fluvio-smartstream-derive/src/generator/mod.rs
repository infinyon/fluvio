use proc_macro2::TokenStream;
use crate::{SmartStreamConfig, SmartStreamFn, SmartStreamKind};

mod filter;
mod map;
mod flatmap;
mod aggregate;

pub fn generate_smartstream(config: &SmartStreamConfig, func: &SmartStreamFn) -> TokenStream {
    match config.kind {
        SmartStreamKind::Filter => self::filter::generate_filter_smartstream(func),
        SmartStreamKind::Map => self::map::generate_map_smartstream(func),
        SmartStreamKind::Aggregate => self::aggregate::generate_aggregate_smartstream(func),
        SmartStreamKind::Flatmap => self::flatmap::generate_flatmap_smartstream(func),
    }
}
