use schemars::JsonSchema;

use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Deserialize, Eq, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}
