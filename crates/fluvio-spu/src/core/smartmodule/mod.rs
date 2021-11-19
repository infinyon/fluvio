mod metadata;

pub use self::metadata::SmartModuleLocalStore;

use std::sync::Arc;

pub type SharedSmartModuleLocalStore = Arc<SmartModuleLocalStore>;
