use std::collections::BTreeMap;
//
//  Peer Spus (all spus in the system, received from Sc)
//      >>> define what each element of SPU is used for
//
use std::sync::Arc;
use std::fmt::Display;
use std::fmt::Debug;
use std::sync::RwLockReadGuard;

use tracing::trace;
use tracing::debug;
use tracing::error;
use tracing::instrument;

use fluvio_controlplane_metadata::message::*;
use fluvio_protocol::{Decoder, Encoder};

use flv_util::actions::Actions;
use flv_util::SimpleConcurrentBTreeMap;

pub trait Spec {
    const LABEL: &'static str;
    type Key: Ord + Clone + ToString;

    fn key(&self) -> &Self::Key;

    fn key_owned(&self) -> Self::Key;
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum SpecChange<S> {
    Add(S),
    Mod(S, S), // new, old
    Delete(S),
}

impl<S> Display for SpecChange<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecChange::Add(_s) => write!(f, "Add"),
            SpecChange::Mod(_new, _old) => write!(f, "Mod"),
            SpecChange::Delete(_s) => write!(f, "Delete"),
        }
    }
}

#[derive(Debug)]
pub struct LocalStore<S>(SimpleConcurrentBTreeMap<S::Key, S>)
where
    S: Spec;

// -----------------------------------
// PeerSpus
// -----------------------------------

impl<S> Default for LocalStore<S>
where
    S: Spec,
{
    fn default() -> Self {
        Self(SimpleConcurrentBTreeMap::new())
    }
}

impl<S> ::std::cmp::PartialEq for LocalStore<S>
where
    S: Spec + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        for (name, local_spu) in self.0.read().iter() {
            let other_list = other.0.read();
            let other_spu = match other_list.get(name) {
                Some(val) => val,
                None => return false,
            };
            if local_spu != other_spu {
                return false;
            }
        }
        true
    }
}

impl<S> LocalStore<S>
where
    S: Spec,
{
    #[allow(unused)]
    pub fn inner_store(&self) -> &SimpleConcurrentBTreeMap<S::Key, S> {
        &self.0
    }

    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// insert new, if there is existing entry, return it
    pub fn insert(&self, spec: S) -> Option<S> {
        self.0.write().insert(spec.key_owned(), spec)
    }

    pub fn delete(&self, id: &S::Key) -> Option<S> {
        self.0.write().remove(id)
    }

    pub fn read(&self) -> RwLockReadGuard<BTreeMap<S::Key, S>> {
        self.0.read()
    }

    #[allow(dead_code)]
    pub fn contains_key(&self, key: &S::Key) -> bool {
        self.0.read().contains_key(key)
    }

    #[allow(dead_code)]
    pub fn count(&self) -> i32 {
        self.0.read().len() as i32
    }
}

impl<S> LocalStore<S>
where
    S: Spec + Clone + PartialEq + Debug + Encoder + Decoder,
    S::Key: Display,
{
    /// apply either all or changes, all takes precedent
    /// TODO: this should be convert to enum
    #[instrument(skip(self,all,changes), fields(spec = S::LABEL, all = all.len()))]
    pub fn apply(&self, all: Vec<S>, changes: Vec<Message<S>>) -> Actions<SpecChange<S>> {
        if !all.is_empty() {
            trace!("received spu all items: {:#?}", all);
            self.sync_all(all)
        } else {
            trace!("received spu change items: {:#?}", changes);
            self.apply_changes(changes)
        }
    }

    /// Sync with source of truth.
    /// Returns diff as Change
    #[instrument(skip(self, source_specs), fields(spec = S::LABEL, command_count = source_specs.len()))]
    pub fn sync_all(&self, source_specs: Vec<S>) -> Actions<SpecChange<S>> {
        let (mut add_cnt, mut mod_cnt, mut del_cnt, mut skip_cnt) = (0, 0, 0, 0);
        let mut local_keys = self.all_keys();
        let mut actions = Actions::default();

        debug!(local_keys = local_keys.len(), "sync all");
        // debug!(
        //     spec_label = S::LABEL,
        //     command_count = source_specs.len(),
        //     "apply all commands"
        // );

        for new_spu in source_specs {
            let id = new_spu.key_owned();

            if let Some(old_spu) = self.insert(new_spu.clone()) {
                if old_spu == new_spu {
                    trace!(spu_key = &*format!("{}", new_spu.key()), "no changes");
                } else {
                    actions.push(SpecChange::Mod(new_spu, old_spu));
                    mod_cnt += 1;
                }
                local_keys.retain(|s| *s != id); // remove visited id
            } else {
                actions.push(SpecChange::Add(new_spu));
                add_cnt += 1;
            }
        }

        // remove any unseen id
        for old_id in local_keys {
            if let Some(old_spu) = self.delete(&old_id) {
                del_cnt += 1;
                actions.push(SpecChange::Delete(old_spu));
            } else {
                error!("delete failed during apply all spu");
                skip_cnt += 1;
            }
        }

        trace!(
            spec_label = S::LABEL,
            add_count = add_cnt,
            mod_count = mod_cnt,
            del_count = del_cnt,
            skip_count = skip_cnt,
            "Apply all spec changes",
        );

        actions
    }

    /// apply changes coming from sc which generates spec change actions
    #[instrument(level = "TRACE",skip(self, changes), fields(spec = S::LABEL, change_count = changes.len()))]
    pub fn apply_changes(&self, changes: Vec<Message<S>>) -> Actions<SpecChange<S>> {
        let (mut add_cnt, mut mod_cnt, mut del_cnt, mut skip_cnt) = (0, 0, 0, 0);
        // debug!(
        //     spec_label = S::LABEL,
        //     change_count = changes.len(),
        //     "apply changes"
        // );
        let mut actions = Actions::default();

        for change in changes {
            match change.header {
                MsgType::UPDATE => {
                    let new_spu = change.content;
                    if let Some(old_spu) = self.insert(new_spu.clone()) {
                        if old_spu == new_spu {
                            trace!("no changes: {}", new_spu.key());
                        } else {
                            actions.push(SpecChange::Mod(new_spu, old_spu));
                            mod_cnt += 1;
                        }
                    } else {
                        actions.push(SpecChange::Add(new_spu));
                        add_cnt += 1;
                    }
                }
                MsgType::DELETE => {
                    let delete_spu = change.content;
                    if let Some(old_spu) = self.delete(delete_spu.key()) {
                        del_cnt += 1;
                        actions.push(SpecChange::Delete(old_spu));
                    } else {
                        error!("delete failed: {}", delete_spu.key());
                        skip_cnt += 1;
                    }
                }
            }
        }

        trace!(
            spec_label = S::LABEL,
            add_count = add_cnt,
            mod_count = mod_cnt,
            del_count = del_cnt,
            skip_count = skip_cnt,
            "change count",
        );

        actions
    }

    pub fn spec(&self, key: &S::Key) -> Option<S> {
        self.0.read().get(key).cloned()
    }

    pub fn all_keys(&self) -> Vec<S::Key> {
        self.0.read().keys().cloned().collect()
    }

    #[allow(dead_code)]
    pub fn all_values(&self) -> Vec<S> {
        self.0.read().values().cloned().collect()
    }
}

// -----------------------------------
//  Unit Tests
// -----------------------------------

#[cfg(test)]
pub mod test {
    use fluvio_controlplane_metadata::spu::SpuSpec;
    use fluvio_controlplane_metadata::message::SpuMsg;

    use crate::core::SpuLocalStore;
    use crate::core::SpecChange;

    #[test]
    fn test_sync_all() {
        let spu_store = SpuLocalStore::default().bulk_add(vec![5000, 5001, 5003]);

        let source = vec![5000.into(), 5002.into(), SpuSpec::new(5001).set_custom()];

        // should generate new(5002),mod(5001),del(5003)
        let mut actions = spu_store.sync_all(source);
        assert_eq!(actions.count(), 3);
        assert_eq!(actions.pop_front().unwrap(), SpecChange::Add(5002.into()));
        assert_eq!(
            actions.pop_front().unwrap(),
            SpecChange::Mod(SpuSpec::new(5001).set_custom(), 5001.into())
        );
        assert_eq!(
            actions.pop_front().unwrap(),
            SpecChange::Delete(5003.into())
        );

        assert_eq!(spu_store.count(), 3);
        assert_eq!(spu_store.spec(&5000).unwrap(), 5000.into());
        assert_eq!(
            spu_store.spec(&5001).unwrap(),
            SpuSpec::new(5001).set_custom()
        );
        assert_eq!(spu_store.spec(&5002).unwrap(), 5002.into());
    }

    #[test]
    fn test_apply_changes() {
        let spu_store = SpuLocalStore::default().bulk_add(vec![5000, 5001, 5003]);

        let changes = vec![
            SpuMsg::update(5002.into()),
            SpuMsg::update(SpuSpec::new(5001).set_custom()),
            SpuMsg::delete(5003.into()),
        ];

        // should generate new(5002),mod(5001),del(5003)
        let mut actions = spu_store.apply_changes(changes);
        assert_eq!(actions.count(), 3);
        assert_eq!(actions.pop_front().unwrap(), SpecChange::Add(5002.into()));
        assert_eq!(
            actions.pop_front().unwrap(),
            SpecChange::Mod(SpuSpec::new(5001).set_custom(), 5001.into())
        );
        assert_eq!(
            actions.pop_front().unwrap(),
            SpecChange::Delete(5003.into())
        );

        assert_eq!(spu_store.count(), 3);
        assert_eq!(spu_store.spec(&5000).unwrap(), 5000.into());
        assert_eq!(
            spu_store.spec(&5001).unwrap(),
            SpuSpec::new(5001).set_custom()
        );
        assert_eq!(spu_store.spec(&5002).unwrap(), 5002.into());
    }
}
