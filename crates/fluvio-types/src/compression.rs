#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}
