//
//  Peer Spus (all spus in the system, received from Sc)
//      >>> define what each element of SPU is used for
//

use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_types::SpuId;

use crate::core::Spec;
use crate::core::LocalStore;

impl Spec for SpuSpec {
    const LABEL: &'static str = "SPU";

    type Key = SpuId;

    fn key(&self) -> &Self::Key {
        &self.id
    }

    fn key_owned(&self) -> Self::Key {
        self.id
    }
}

pub type SpuLocalStore = LocalStore<SpuSpec>;

impl SpuLocalStore {
    #[cfg(test)]
    pub fn indexed_by_id(&self) -> std::collections::BTreeMap<SpuId, SpuSpec> {
        let mut map: std::collections::BTreeMap<SpuId, SpuSpec> = std::collections::BTreeMap::new();

        for spu in self.inner_store().read().values() {
            map.insert(spu.id, spu.clone());
        }

        map
    }

    #[cfg(test)]
    pub fn bulk_add(self, spus: Vec<i32>) -> Self {
        for id in spus {
            self.insert(id.into());
        }
        self
    }
}

// -----------------------------------
//  Unit Tests
// -----------------------------------

#[cfg(test)]
pub mod test {
    use std::collections::BTreeMap;

    use crate::core::SpuLocalStore;

    #[test]
    fn test_indexed_by_id() {
        let spus = SpuLocalStore::default().bulk_add(vec![5000, 5001, 5002]);

        // run test
        let indexed_spus = spus.indexed_by_id();

        // check result
        let mut expected_indexed_spus = BTreeMap::default();
        expected_indexed_spus.insert(5000, spus.spec(&5000).unwrap());
        expected_indexed_spus.insert(5001, spus.spec(&5001).unwrap());
        expected_indexed_spus.insert(5002, spus.spec(&5002).unwrap());

        assert_eq!(indexed_spus, expected_indexed_spus);
    }

    #[test]
    fn test_peer_spu_routines() {
        let spus = SpuLocalStore::default().bulk_add(vec![5000, 5001, 5002]);

        let spu_5000 = spus.spec(&5000).unwrap();
        let spu_5001 = spus.spec(&5001).unwrap();
        let spu_5002 = spus.spec(&5002).unwrap();

        // test >> count()
        let count = spus.count();
        assert_eq!(count, 3);

        // test >> names()
        let names = spus.all_keys();
        assert_eq!(names, vec![5000, 5001, 5002]);

        // test >> all_spus()
        let all_spus = spus.all_values();
        assert_eq!(all_spus.len(), 3);
        assert_eq!(all_spus[0], spu_5000);
        assert_eq!(all_spus[1], spu_5001);
        assert_eq!(all_spus[2], spu_5002);

        // test >> delete()
        spus.delete(&5001);
        let remaining_spus = spus.all_values();
        assert_eq!(remaining_spus.len(), 2);
        assert_eq!(remaining_spus[0], spu_5000);
        assert_eq!(remaining_spus[1], spu_5002);
    }
}
