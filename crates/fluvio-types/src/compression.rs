use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Deserialize, Eq, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}
