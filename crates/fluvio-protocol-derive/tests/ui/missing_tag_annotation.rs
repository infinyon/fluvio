use fluvio_protocol_derive::{Decoder, Encoder};

#[derive(Clone, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    Predefined(String),
    AdHoc(Vec<u8>),
}
