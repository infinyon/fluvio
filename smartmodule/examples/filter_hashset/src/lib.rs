use std::{
    sync::OnceLock,
    collections::{
        BTreeMap,
        btree_map::Entry::{Vacant, Occupied},
    },
};

use fluvio_smartmodule::{
    smartmodule, SmartModuleRecord, Result, dataplane::smartmodule::SmartModuleExtraParams, eyre,
};

static SET: OnceLock<BoundedHashSet<String>> = OnceLock::new();

#[smartmodule(filter)]
pub fn filter(record: &SmartModuleRecord) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(get_mut_set()?.insert(string.to_owned()))
}

#[smartmodule(look_back)]
pub fn look_back(record: &SmartModuleRecord) -> Result<()> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    get_mut_set()?.insert(string.to_owned());
    Ok(())
}

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    let count: usize = if let Some(count) = params.get("count") {
        count.parse()?
    } else {
        usize::MAX - 1
    };
    let _ = SET.set(BoundedHashSet::new(count));
    Ok(())
}

fn get_mut_set() -> Result<&'static mut BoundedHashSet<String>> {
    let ptr: *const BoundedHashSet<String> = SET
        .get()
        .ok_or_else(|| eyre!("hashset must be set before"))?;
    Ok(unsafe { &mut *(ptr as *mut BoundedHashSet<String>) })
}

struct BoundedHashSet<K: Ord> {
    inner: BTreeMap<K, usize>,
    limit: usize,
    seq: usize,
}

impl<K: Ord + Clone + std::fmt::Debug> BoundedHashSet<K> {
    fn new(limit: usize) -> Self {
        Self {
            inner: Default::default(),
            limit,
            seq: Default::default(),
        }
    }

    fn insert(&mut self, value: K) -> bool {
        self.seq = self.seq.saturating_add(1);
        let res = match self.inner.entry(value) {
            Vacant(entry) => {
                entry.insert(self.seq);
                true
            }
            Occupied(_) => false,
        };
        if self.inner.len() > self.limit {
            self.remove_first();
        }
        res
    }

    fn remove_first(&mut self) {
        let min: Option<(&K, &usize)> = self.inner.iter().min_by(|x, y| x.1.cmp(y.1));
        if let Some((key, _)) = min {
            let key = key.clone();
            self.inner.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set() {
        //given
        let mut set = BoundedHashSet::new(3);

        //when
        assert!(set.insert("1"));
        assert!(set.insert("3"));
        assert!(set.insert("2"));
        assert!(!set.insert("3"));
        assert!(!set.insert("1"));
        assert!(set.insert("5"));
        assert!(set.insert("1"));

        //then
        assert_eq!(
            set.inner.keys().cloned().collect::<Vec<&str>>(),
            &["1", "2", "5"]
        );
    }
}
