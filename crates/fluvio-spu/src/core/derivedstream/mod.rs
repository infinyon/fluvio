mod metadata;
pub use metadata::*;

use std::sync::Arc;

pub type SharedStreamStreamLocalStore = Arc<DerivedStreamStore>;
