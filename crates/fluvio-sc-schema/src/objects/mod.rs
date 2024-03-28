mod create;
mod delete;
mod list;
mod watch;
mod metadata;

// backward compatibility with classic protocol. this should go away once we deprecate classic
pub mod classic;

pub use create::*;
pub use delete::*;
pub use list::*;
pub use watch::*;
pub use metadata::*;

pub(crate) const COMMON_VERSION: i16 = 13; // from now, we use a single version for all objects
pub(crate) const DYN_OBJ: i16 = 11; // version indicate dynamic object

#[cfg(test)]
mod test;
