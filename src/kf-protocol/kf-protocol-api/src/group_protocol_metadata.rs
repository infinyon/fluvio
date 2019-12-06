use std::io::Error;
use std::io::ErrorKind;

use serde::{Serialize, Deserialize};

use kf_protocol::{Encoder, Decoder};
use kf_protocol::bytes::{BufMut, Buf};
use kf_protocol::Version;

// -----------------------------------
// ProtocolMetadata
// -----------------------------------

/*
Reverse Engineered

    ProtocolMetadata {
        // 0x00, 0x00, 0x00, 0x10
        // [         16          ] byte array length
        len: i32,

        // 0x00, 0x00, ??
        reserved_i16: i16,

        // 0x00, 0x00, 0x00, 0x01
        // [         1           ] topics array length
        // 0x00, 0x04, 0x74, 0x65, 0x73, 0x74
        // [   len   ] [ t    e     s     t]
        topics: Vec<String>,

        pub reserved_i32: i32,
    }

*/

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct ProtocolMetadata {
    pub content: Option<Metadata>,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct Metadata {
    pub reserved_i16: i16,
    pub topics: Vec<String>,
    pub reserved_i32: i32,
}

impl Encoder for ProtocolMetadata {
    fn write_size(&self, version: Version) -> usize {
        let mut len = if let Some(content) = &self.content {
            content.reserved_i16.write_size(version)
                + content.topics.write_size(version)
                + content.reserved_i32.write_size(version)
        } else {
            0
        };
        len += 4;
        len
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        if dest.remaining_mut() < 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough capacity for length",
            ));
        }

        let length = (self.write_size(version) as i32) - 4;
        length.encode(dest, version)?;
        if let Some(content) = &self.content {
            content.reserved_i16.encode(dest, version)?;
            content.topics.encode(dest, version)?;
            content.reserved_i32.encode(dest, version)?;
        }

        Ok(())
    }
}

impl Decoder for ProtocolMetadata {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        if src.remaining() < 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough buf for i32",
            ));
        }

        let mut len: i32 = 0;
        len.decode(src, version)?;
        if len > 0 {
            if src.remaining() < len as usize {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "not enough buf to decode metadata",
                ));
            }

            let mut reserved_i16: i16 = 0;
            let mut topics: Vec<String> = vec![];
            let mut reserved_i32: i32 = 0;

            reserved_i16.decode(src, version)?;
            topics.decode(src, version)?;
            reserved_i32.decode(src, version)?;

            let metadata = Metadata {
                reserved_i16,
                topics,
                reserved_i32,
            };

            *self = Self {
                content: Some(metadata),
            };
        }
        Ok(())
    }
}

// -----------------------------------
// Test Cases
// -----------------------------------

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_group_protocol_metadata_decoding() {
        let data = [
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65,
            0x73, 0x74, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut value = ProtocolMetadata::default();
        let mut cursor = &mut Cursor::new(data);
        let result = value.decode(&mut cursor, 4);
        assert!(result.is_ok());

        let metadata = Metadata {
            reserved_i16: 0,
            topics: vec!["test".to_owned()],
            reserved_i32: 0,
        };
        let expected_value = ProtocolMetadata {
            content: Some(metadata),
        };

        assert_eq!(value, expected_value);
    }

    #[test]
    fn test2_metadata_group_decoding() {
        let data = [
            0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x74, 0x65,
            0x73, 0x74, 0x32, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut value = ProtocolMetadata::default();
        let mut cursor = &mut Cursor::new(data);
        let result = value.decode(&mut cursor, 4);
        assert!(result.is_ok());

        let metadata = Metadata {
            reserved_i16: 0,
            topics: vec!["test2".to_owned()],
            reserved_i32: 0,
        };
        let expected_value = ProtocolMetadata {
            content: Some(metadata),
        };

        assert_eq!(value, expected_value);
    }

    #[test]
    fn test_metadata_group_encoding() {
        let mut data: Vec<u8> = vec![];
        let exected_data = [
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65,
            0x73, 0x74, 0x00, 0x00, 0x00, 0x00,
        ];

        let metadata = Metadata {
            reserved_i16: 0,
            topics: vec!["test".to_owned()],
            reserved_i32: 0,
        };

        let protocol = ProtocolMetadata {
            content: Some(metadata),
        };

        let result = protocol.encode(&mut data, 4);
        assert!(result.is_ok());

        assert_eq!(data, exected_data);
    }

}
