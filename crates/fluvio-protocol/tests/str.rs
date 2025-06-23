use std::io::Cursor;

use fluvio_protocol::{Decoder, Encoder};

#[test]
fn test_encode_str() {
    let value = "hello";
    assert_eq!(value.write_size(0), 7); // 7 bytes for the string "hello"
    let mut dest = Vec::new();
    value.encode(&mut dest, 0).expect("Failed to encode string");

    // decode the string back to its original form
    let mut buf = Cursor::new(dest);
    let decoded = String::decode_from(&mut buf, 0).expect("Failed to decode string");
    assert_eq!(decoded, "hello");
}
