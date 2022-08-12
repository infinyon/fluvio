use fluvio_protocol::Encoder;

fn main() {}

#[derive(Encoder)]
struct PassTupleStruct(u16, String);

#[derive(Encoder)]
struct PassNamedStruct {
    number: u16,
    string: String,
}

#[repr(u16)]
#[derive(Encoder)]
#[fluvio(encode_discriminant)]
enum PassUnitEnum {
    One = 1,
    Two = 2,
    Three = 3,
}

#[derive(Encoder)]
enum PassTupleEnum {
    First(String),
    Second(u16),
    Third(Vec<u8>),
}

#[derive(Encoder)]
enum PassNamedEnum {
    Alpha { name: String, number: i32 },
    Beta { data: Vec<u8> },
}