// experimental serde adoptor
// it works with most of kafka data types
// but doesn't work with optional string and varint

mod de;
mod error;

pub use self::error::Error;
pub use self::error::ErrorKind;