use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Deserialize, Eq, Serialize, PartialEq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}
