mod metadata;

pub use self::metadata::SpuLocalStore;

use std::sync::Arc;

pub type SharedSpuLocalStore = Arc<SpuLocalStore>;
