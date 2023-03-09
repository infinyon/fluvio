use fluvio_protocol::Decoder;

fn main() {}

#[derive(Default, Decoder)]
struct PassTupleStruct(u16, String);

#[derive(Default, Decoder)]
struct PassNamedStruct {
    number: u16,
    string: String,
}

#[repr(u16)]
#[derive(Decoder)]
#[fluvio(encode_discriminant)]
enum PassUnitEnum {
    One = 1,
    Two = 2,
    #[fluvio(tag = 5)]
    Three = 3,
}

impl Default for PassUnitEnum {
    fn default() -> Self {
        Self::One
    }
}

#[derive(Decoder)]
enum PassTupleEnum {
    #[fluvio(tag = 0)]
    First(String),
    #[fluvio(tag = 2)]
    Second(u16),
    #[fluvio(tag = 50)]
    Third(Vec<u8>),
}

impl Default for PassTupleEnum {
    fn default() -> Self {
        Self::First(Default::default())
    }
}

#[derive(Decoder)]
enum PassNamedEnum {
    #[fluvio(tag = 0)]
    Alpha {
        name: String,
        number: i32,
    },
    #[fluvio(tag = 30)]
    Beta {
        data: Vec<u8>,
    },
}

impl Default for PassNamedEnum {
    fn default() -> Self {
        Self::Alpha {
            name: Default::default(),
            number: Default::default(),
        }
    }
}