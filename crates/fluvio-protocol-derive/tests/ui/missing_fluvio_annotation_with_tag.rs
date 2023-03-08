use fluvio_protocol_derive::{Decoder, Encoder};

#[derive(Clone, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    #[fluvio]
    Predefined(String),
    #[fluvio]
    AdHoc(Vec<u8>),
}
