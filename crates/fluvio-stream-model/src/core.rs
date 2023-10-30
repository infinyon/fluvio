pub use core_model::*;
pub use context::*;

mod context {

    use std::collections::hash_map::Entry;
    use std::fmt;
    use std::fmt::Display;
    use std::collections::HashMap;

    pub type DefaultMetadataContext = MetadataContext<String>;

    pub trait MetadataItem:
        Clone + Default + fmt::Debug + PartialEq + Send + Sync + 'static
    {
        type UId: PartialEq;

        fn uid(&self) -> &Self::UId;

        /// checkif item is newer
        fn is_newer(&self, another: &Self) -> bool;

        /// if object is process of being deleted
        fn is_being_deleted(&self) -> bool {
            false
        }

        /// set string labels
        fn set_labels<T: Into<String>>(self, _labels: Vec<(T, T)>) -> Self {
            self
        }

        /// get string labels
        fn get_labels(&self) -> HashMap<String, String> {
            HashMap::new()
        }
    }

    pub trait MetadataRevExtension: MetadataItem {
        // return next revision
        fn next_rev(&self) -> Self;
    }

    impl MetadataItem for u32 {
        type UId = u32;

        fn uid(&self) -> &Self::UId {
            self
        }

        fn is_newer(&self, another: &Self) -> bool {
            self > another
        }
    }

    impl MetadataItem for u64 {
        type UId = u64;

        fn uid(&self) -> &Self::UId {
            self
        }

        fn is_newer(&self, another: &Self) -> bool {
            self > another
        }
    }

    #[derive(Default, Debug, Clone, Eq, PartialEq)]
    pub struct MetadataContext<C> {
        item: C,
        owner: Option<C>,
        children: HashMap<String, Vec<C>>,
    }

    impl<C> From<C> for MetadataContext<C> {
        fn from(item: C) -> Self {
            Self {
                item,
                owner: None,
                children: Default::default(),
            }
        }
    }

    impl<C> MetadataContext<C> {
        pub fn new(item: C, owner: Option<C>) -> Self {
            Self {
                item,
                owner,
                children: Default::default(),
            }
        }

        pub fn item(&self) -> &C {
            &self.item
        }

        pub fn item_mut(&mut self) -> &mut C {
            &mut self.item
        }

        pub fn set_item(&mut self, item: C) {
            self.item = item;
        }

        pub fn item_owned(self) -> C {
            self.item
        }

        pub fn into_parts(self) -> (C, Option<C>, HashMap<String, Vec<C>>) {
            (self.item, self.owner, self.children)
        }

        pub fn owner(&self) -> Option<&C> {
            self.owner.as_ref()
        }
        pub fn set_owner(&mut self, ctx: C) {
            self.owner = Some(ctx);
        }
        pub fn children(&self) -> &HashMap<String, Vec<C>> {
            &self.children
        }
        pub fn set_children(&mut self, children: HashMap<String, Vec<C>>) {
            self.children = children;
        }
    }

    impl<C> MetadataContext<C>
    where
        C: MetadataItem,
    {
        pub fn create_child(&self) -> Self {
            Self {
                item: C::default(),
                owner: Some(self.item.clone()),
                children: Default::default(),
            }
        }

        pub fn set_labels<T: Into<String>>(self, labels: Vec<(T, T)>) -> Self {
            Self {
                item: self.item.set_labels(labels),
                owner: self.owner,
                children: Default::default(),
            }
        }

        pub fn put_child<S: Into<String>>(&mut self, kind: S, child: C) {
            match self.children.entry(kind.into()) {
                Entry::Occupied(mut entry) => {
                    let vec = entry.get_mut();
                    if !vec.contains(&child) {
                        vec.push(child);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(vec![child]);
                }
            }
        }
        pub fn remove_child<S: Into<String>>(&mut self, kind: S, child: &C) {
            match self.children.entry(kind.into()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().retain(|i| !i.eq(child));
                    if entry.get().is_empty() {
                        entry.remove_entry();
                    }
                }
                Entry::Vacant(_) => {}
            }
        }
    }

    impl<C> MetadataContext<C>
    where
        C: MetadataRevExtension,
    {
        pub fn next_rev(&self) -> Self {
            Self::new(self.item.next_rev(), None)
        }
    }

    impl<C> Display for MetadataContext<C>
    where
        C: MetadataItem,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{:#?}", self.item)
        }
    }
}

mod core_model {

    use std::fmt::{Debug, Display};
    use std::hash::Hash;
    use std::str::FromStr;

    /// metadata driver
    pub trait MetadataStoreDriver {
        type Metadata;
    }

    #[cfg(not(feature = "use_serde"))]
    pub trait Spec: Default + Debug + Clone + PartialEq + Send + Sync + 'static {
        const LABEL: &'static str;
        type Status: Status;
        type Owner: Spec;
        type IndexKey: Debug + Eq + Hash + Clone + ToString + FromStr + Display + Send + Sync;
    }

    #[cfg(feature = "use_serde")]
    pub trait Spec:
        Default
        + Debug
        + Clone
        + PartialEq
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
    {
        const LABEL: &'static str;
        type Status: Status + serde::Serialize + serde::de::DeserializeOwned;
        type Owner: Spec;
        type IndexKey: Debug + Eq + Hash + Clone + ToString + FromStr + Display + Send + Sync;
    }

    pub trait Status:
        Default + Debug + Clone + ToString + Display + PartialEq + Send + Sync
    {
    }

    /// for deleting objects
    pub trait Removable {
        type DeleteKey;
    }

    /// marker trait for creating
    pub trait Creatable {}

    /// Represents some metadata object
    pub struct MetadataObj<S, P>
    where
        P: MetadataStoreDriver,
        S: Spec,
    {
        pub name: String,
        pub metadata: P::Metadata,
        pub spec: S,
        pub status: S::Status,
    }
}
