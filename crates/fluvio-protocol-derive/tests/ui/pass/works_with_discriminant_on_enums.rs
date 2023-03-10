use fluvio_protocol_derive::{Decoder, Encoder};

#[derive(Clone, Default, Encoder, Decoder)]
#[fluvio(encode_discriminant)]
pub enum SmartModuleInvocationWasm {
    #[default]
    Predefined = 0,
    AdHoc = 1,
}

fn main() {}
