//!
//! # Hex Dump API
//!
//! Converts a vector of bytst to hex dump string format
//!

/// Takes a u8 array of bytes and converts to hex dump
#[allow(clippy::needless_range_loop)]
pub fn bytes_to_hex_dump(record: &[u8]) -> String {
    use std::fmt::Write;

    let cols = 16;
    let record_cnt = record.len();
    let mut result = String::new();
    let mut collector = String::new();

    for row_idx in 0..record_cnt {
        // column index
        if row_idx % cols == 0 {
            write!(result, "{row_idx:08x}").unwrap();
        }

        // spacing half way
        if row_idx % (cols / 2) == 0 {
            result.push(' ');
        }

        // convert and add character to collector
        collector.push_str(&byte_to_string(&record[row_idx]));

        // push binary
        write!(result, " {:02x}", record[row_idx]).unwrap();

        // push characters
        if (row_idx + 1) % cols == 0 {
            writeln!(result, "  |{collector}|").unwrap();
            collector = String::new();
        }
    }

    // if collect not empty, fill-in gap and add characters
    if !collector.is_empty() {
        let last_char_idx = record_cnt % cols;
        if last_char_idx <= cols / 2 {
            result.push(' ');
        }
        for _ in last_char_idx..cols {
            collector.push(' ');
            result.push_str("   ");
        }

        writeln!(result, "  |{collector}|").unwrap();
    }

    result
}

/// Converts a byte to string character
fn byte_to_string(byte: &u8) -> String {
    if 0x20 <= *byte && *byte < 0x7f {
        format!("{}", *byte as char)
    } else if *byte == 0xa {
        ".".to_owned()
    } else {
        " ".to_owned()
    }
}

/// Return separator for hex dump
pub fn hex_dump_separator() -> String {
    "------------------------------------------------------------------------------\n".to_owned()
}

#[cfg(test)]
mod test {
    use super::bytes_to_hex_dump;

    #[test]
    fn test_bytes_to_hex_dump() {
        let records: Vec<u8> = vec![
            123, 10, 32, 32, 32, 32, 34, 112, 97, 114, 116, 105, 116, 105, 111, 110, 115, 34, 58,
            32, 91, 10, 32, 32, 32, 32, 32, 32, 32, 32, 123, 10, 32, 32, 32, 32, 32, 32, 32, 32,
            32, 32, 32, 32, 34, 105, 100, 34, 58, 32, 48, 44, 10, 32, 32, 32, 32, 32, 32, 32, 32,
            32, 32, 32, 32, 34, 114, 101, 112, 108, 105, 99, 97, 115, 34, 58, 32, 91, 10, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 53, 48, 48, 49, 44, 10, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 53, 48, 48, 50, 44, 10, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 53, 48, 48, 51, 10, 32, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 93, 10, 32, 32, 32, 32, 32, 32, 32, 32, 125, 10,
            32, 32, 32, 32, 93, 10, 125,
        ];

        let expected = r#"00000000  7b 0a 20 20 20 20 22 70  61 72 74 69 74 69 6f 6e  |{.    "partition|
00000010  73 22 3a 20 5b 0a 20 20  20 20 20 20 20 20 7b 0a  |s": [.        {.|
00000020  20 20 20 20 20 20 20 20  20 20 20 20 22 69 64 22  |            "id"|
00000030  3a 20 30 2c 0a 20 20 20  20 20 20 20 20 20 20 20  |: 0,.           |
00000040  20 22 72 65 70 6c 69 63  61 73 22 3a 20 5b 0a 20  | "replicas": [. |
00000050  20 20 20 20 20 20 20 20  20 20 20 20 20 20 20 35  |               5|
00000060  30 30 31 2c 0a 20 20 20  20 20 20 20 20 20 20 20  |001,.           |
00000070  20 20 20 20 20 35 30 30  32 2c 0a 20 20 20 20 20  |     5002,.     |
00000080  20 20 20 20 20 20 20 20  20 20 20 35 30 30 33 0a  |           5003.|
00000090  20 20 20 20 20 20 20 20  20 20 20 20 5d 0a 20 20  |            ].  |
000000a0  20 20 20 20 20 20 7d 0a  20 20 20 20 5d 0a 7d     |      }.    ].} |
"#;

        let result = bytes_to_hex_dump(&records);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bytes_to_hex_dump_half_row() {
        let records: Vec<u8> = vec![123, 10, 32, 32, 32, 32, 34, 112];

        let expected = r#"00000000  7b 0a 20 20 20 20 22 70                           |{.    "p        |
"#;

        let result = bytes_to_hex_dump(&records);
        assert_eq!(result, expected);
    }
}
