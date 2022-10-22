mod bytebuf;
mod decoder;
mod encoder;
mod varint;
mod zerocopy;

pub use self::bytebuf::ByteBuf;
pub use self::decoder::Decoder;
pub use self::decoder::DecoderVarInt;
pub use self::encoder::Encoder;
pub use self::encoder::EncoderVarInt;

pub type Version = i16;

#[cfg(test)]
#[allow(unused)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MyMessage {
        value1: [u8; 2],
        value2: [u8; 4], // u32
    }

    impl MyMessage {
        fn value1(&self) -> i16 {
            i16::from_be_bytes(self.value1)
        }

        fn set_value1(&mut self, val: i16) {
            self.value1 = val.to_be_bytes()
        }
    }

    #[test]
    fn test_message() {
        let mut m = MyMessage::default();

        m.set_value1(10);
        let bytes = m.value1;
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[1], 10);
    }
}
