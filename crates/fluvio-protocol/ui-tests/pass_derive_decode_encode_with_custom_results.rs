use fluvio_protocol::{Encoder, Decoder};

pub type Result = std::result::Result<(), ()>;

fn main() {}

#[derive(Default, Decoder, Encoder)]
struct PassTupleStruct(String);
