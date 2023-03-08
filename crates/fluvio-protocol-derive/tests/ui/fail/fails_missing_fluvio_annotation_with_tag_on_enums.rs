use fluvio_protocol_derive::{Decoder, Encoder};

#[derive(Clone, Default, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    #[default]
    #[fluvio(min_version = 1)]
    Predefined,
    #[fluvio(min_version = 2)]
    AdHoc,
}

fn main() {}
