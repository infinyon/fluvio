use fluvio_protocol_derive::{Decoder, Encoder};

#[derive(Clone, Default, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    #[default]
    #[fluvio(tag = 0)]
    Predefined,
    #[fluvio(tag = 1)]
    AdHoc,
}

fn main() {}
