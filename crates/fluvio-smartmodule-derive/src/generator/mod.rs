mod filter;
mod map;
mod array_map;
mod filter_map;
mod aggregate;
mod init;
mod transform;
mod look_back;

pub mod opt;

use proc_macro2::TokenStream;
use quote::quote;

use crate::ast::RecordKind;
use crate::{SmartModuleConfig, SmartModuleFn, SmartModuleKind};

pub fn generate_smartmodule(config: &SmartModuleConfig, func: &SmartModuleFn) -> TokenStream {
    match config.kind.as_ref().expect("Smartmodule type not set") {
        SmartModuleKind::Filter => self::filter::generate_filter_smartmodule(func),
        SmartModuleKind::Map => self::map::generate_map_smartmodule(func),
        SmartModuleKind::FilterMap => self::filter_map::generate_filter_map_smartmodule(func),
        SmartModuleKind::Aggregate => self::aggregate::generate_aggregate_smartmodule(func),
        SmartModuleKind::ArrayMap => self::array_map::generate_array_map_smartmodule(func),
        SmartModuleKind::Init => self::init::generate_init_smartmodule(func),
        SmartModuleKind::LookBack => self::look_back::generate_look_back_smartmodule(func),
    }
}

/// Generates the `SmartModuleFn` records decoding code based on the `RecordKind`
/// provided by the `SmartModuleFn`. This generator needs at local `input_data`
/// variable to be in scope, which is the raw bytes of the `SmartModuleInput`.
///
/// This `TokenStream` expands to two variables:
///
/// - `smartmodule_input`: The decoded `SmartModuleInput` struct from `input_data`
/// - `records`: The decoded `Vec<Record>` or `Vec<SmartModuleRecord>` from `smartmodule_input`
///
pub fn generate_records_code(sm_func: &SmartModuleFn, kind: &SmartModuleKind) -> TokenStream {
    // Provides the corresponding error enum for the SmartModuleKind
    let sm_input_decode_error_code = match kind {
        SmartModuleKind::Init
        | SmartModuleKind::ArrayMap
        | SmartModuleKind::FilterMap
        | SmartModuleKind::Map
        | SmartModuleKind::Filter
        | SmartModuleKind::Aggregate => quote! {
            use fluvio_smartmodule::dataplane::smartmodule::SmartModuleTransformErrorStatus;

            return SmartModuleTransformErrorStatus::DecodingBaseInput as i32;
        },
        SmartModuleKind::LookBack => quote! {
            use fluvio_smartmodule::dataplane::smartmodule::SmartModuleLookbackErrorStatus;

            return SmartModuleLookbackErrorStatus::DecodingBaseInput as i32;
        },
    };

    let sm_input_code = quote! {
        use fluvio_smartmodule::dataplane::record::{Record, RecordData};
        use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleRecord};
        use fluvio_smartmodule::dataplane::core::{Encoder, Decoder};

        let mut smartmodule_input = SmartModuleInput::default();

        if let Err(_err) = Decoder::decode(&mut smartmodule_input, &mut std::io::Cursor::new(input_data), version) {
            #sm_input_decode_error_code
        }
    };

    match sm_func.record_kind {
        RecordKind::LegacyRecord => quote! {
            #sm_input_code

            let mut records: Vec<Record> = match smartmodule_input.clone().try_into_records(version) {
                Ok(records) => records,
                Err(_) => {
                    #sm_input_decode_error_code
                }
            };
        },
        RecordKind::SmartModuleRecord => quote! {
            #sm_input_code

            let mut records: Vec<SmartModuleRecord> = match smartmodule_input.clone().try_into_smartmodule_records(version) {
                Ok(records) => records,
                Err(_) => {
                    #sm_input_decode_error_code
                }
            };
        },
    }
}
