//!
//! # Key/Value Context
//!
//! Key/Value Contexts are required by KV store for modifications and owner_references.
//! Controller treats these objects as opaque cookies which are converted to Metadata by
//! the KV client.
use k8_metadata::metadata::ObjectMeta;

#[derive(Debug, PartialEq, Clone)]
pub struct KvContext {
    pub item_ctx: Option<ObjectMeta>,
    pub parent_ctx: Option<ObjectMeta>,
}

impl KvContext {
    pub fn with_ctx(mut self, ctx: ObjectMeta) -> Self {
        self.item_ctx = Some(ctx);
        self
    }

    pub fn with_parent_ctx(mut self, ctx: ObjectMeta) -> Self {
        self.parent_ctx = Some(ctx);
        self
    }

    pub fn make_parent_ctx(&self) -> KvContext {
        if self.item_ctx.is_some() {
            KvContext::default().with_parent_ctx(self.item_ctx.as_ref().unwrap().clone())
        } else {
            KvContext::default()
        }
    }
}

impl ::std::default::Default for KvContext {
    fn default() -> Self {
        KvContext {
            item_ctx: None,
            parent_ctx: None,
        }
    }
}
