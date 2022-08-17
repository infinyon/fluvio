mod record;
pub use self::record::*;

mod batch;
mod replica;
pub use batch::*;
pub use replica::*;

pub type Offset = i64;
pub type Size = u32;
pub type Size64 = u64;
