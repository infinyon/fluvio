use proc_macro2::TokenStream;
use crate::{SmartModuleConfig, SmartModuleFn, SmartModuleKind};

mod filter;
mod map;
mod array_map;
mod filter_map;
mod aggregate;
mod init;
mod transform;
pub mod opt;

pub fn generate_smartmodule(config: &SmartModuleConfig, func: &SmartModuleFn) -> TokenStream {
    match config.kind {
        SmartModuleKind::Filter => self::filter::generate_filter_smartmodule(func),
        SmartModuleKind::Map => self::map::generate_map_smartmodule(func),
        SmartModuleKind::FilterMap => self::filter_map::generate_filter_map_smartmodule(func),
        SmartModuleKind::Aggregate => self::aggregate::generate_aggregate_smartmodule(func),
        SmartModuleKind::ArrayMap => self::array_map::generate_array_map_smartmodule(func),
        SmartModuleKind::Init => self::init::generate_init_smartmodule(func),
    }
}
