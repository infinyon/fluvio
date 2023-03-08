use fluvio_protocol_derive::{Decoder, Encoder};

#[derive(Clone, Default, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    #[default]
    #[fluvio(tag = 0)]
    Predefined = 0,
    #[fluvio(tag = 1)]
    AdHoc = 1,
}

fn main() {}
