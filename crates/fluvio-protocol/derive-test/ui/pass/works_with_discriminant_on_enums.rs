use fluvio_protocol::{Decoder, Encoder};

#[derive(Clone, Default, Encoder, Decoder)]
#[fluvio(encode_discriminant)]
pub enum SmartModuleInvocationWasm {
    #[default]
    Predefined = 0,
    AdHoc = 1,
}

fn main() {}
