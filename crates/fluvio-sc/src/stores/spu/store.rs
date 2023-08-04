//!
//! # SC Spu Metadata
//!
//! Spu metadata information cached locally.
//!
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;

use async_trait::async_trait;

use fluvio_types::SpuId;

use crate::spu::*;
use crate::store::*;
use crate::core::*;
use crate::message::*;

pub type SpuLocalStore<C> = LocalStore<SpuSpec, C>;
pub type DefaultSpuStore = SpuLocalStore<u32>;
pub type SharedSpuLocalStore<C> = Arc<SpuLocalStore<C>>;
pub type SpuMetadata<C> = MetadataStoreObject<SpuSpec, C>;
pub type DefaultSpuMd = SpuMetadata<u32>;

pub trait SpuMd<C: MetadataItem> {
    fn quick<J>(spu: (J, SpuId, bool, Option<String>)) -> SpuMetadata<C>
    where
        J: Into<String>;
}

impl<C: MetadataItem> SpuMd<C> for SpuMetadata<C> {
    fn quick<J>(spu: (J, SpuId, bool, Option<String>)) -> SpuMetadata<C>
    where
        J: Into<String>,
    {
        let spec = SpuSpec {
            id: spu.1,
            rack: spu.3,
            ..Default::default()
        };

        let mut status = SpuStatus::default();
        if spu.2 {
            status.set_online();
        }

        SpuMetadata::new(spu.0.into(), spec, status)
    }
}

#[async_trait]
pub trait SpuLocalStorePolicy<C>
where
    C: MetadataItem,
{
    async fn online_status(&self) -> HashSet<SpuId>;

    async fn online_spu_count(&self) -> u32;

    async fn spu_used_for_replica(&self) -> usize;

    async fn online_spu_ids(&self) -> Vec<SpuId>;

    async fn spu_ids(&self) -> Vec<SpuId>;

    async fn online_spus(&self) -> Vec<SpuMetadata<C>>;

    async fn custom_spus(&self) -> Vec<SpuMetadata<C>>;

    async fn get_by_id(&self, id: SpuId) -> Option<SpuMetadata<C>>;

    async fn validate_spu_for_registered(&self, id: SpuId) -> bool;

    async fn all_names(&self) -> Vec<String>;

    async fn table_fmt(&self) -> String;

    async fn spus_in_rack_count(&self) -> u32;

    async fn live_spu_rack_map_sorted(&self) -> Vec<(String, Vec<SpuId>)>;

    async fn online_spu_rack_map(&self) -> BTreeMap<String, Vec<SpuId>>;

    fn online_spus_in_rack(rack_map: &[(String, Vec<SpuId>)]) -> Vec<SpuId>;

    async fn all_spus_to_spu_msgs(&self) -> Vec<SpuMsg>;

    fn quick(spus: Vec<(SpuId, bool, Option<String>)>) -> Self;
}

#[async_trait]
impl<C> SpuLocalStorePolicy<C> for SpuLocalStore<C>
where
    C: MetadataItem + Send + Sync,
{
    // build hashmap of online
    async fn online_status(&self) -> HashSet<SpuId> {
        let mut status = HashSet::new();
        for (_, spu) in self.read().await.iter() {
            if spu.status.is_online() {
                status.insert(spu.spec.id);
            }
        }
        status
    }

    /// count online SPUs
    async fn online_spu_count(&self) -> u32 {
        self.read()
            .await
            .values()
            .filter_map(|spu| {
                if spu.status.is_online() {
                    Some(1)
                } else {
                    None
                }
            })
            .sum()
    }

    /// count spus that can be used for replica
    async fn spu_used_for_replica(&self) -> usize {
        self.count().await
    }

    // retrieve SPU ids.
    async fn online_spu_ids(&self) -> Vec<SpuId> {
        self.read()
            .await
            .values()
            .filter(|spu| spu.status.is_online())
            .map(|spu| spu.spec.id)
            .collect()
    }

    async fn spu_ids(&self) -> Vec<SpuId> {
        let mut ids: Vec<SpuId> = self.read().await.values().map(|spu| spu.spec.id).collect();
        ids.sort_unstable();
        ids
    }

    async fn online_spus(&self) -> Vec<SpuMetadata<C>> {
        self.read()
            .await
            .values()
            .filter(|spu| spu.status.is_online())
            .map(|spu| spu.inner().clone())
            .collect()
    }

    async fn custom_spus(&self) -> Vec<SpuMetadata<C>> {
        self.read()
            .await
            .values()
            .filter_map(|spu| {
                if spu.spec.is_custom() {
                    Some(spu.inner().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /*
     this is now can be get as value().inner_owned()
    pub async fn spu(&self, name: &str) -> Option<SpuMetadata<C>> {
        match self.read().await.get(name) {
            Some(spu) => Some(spu.inner().clone().into()),
            None => None,
        }
    }
    */

    async fn get_by_id(&self, id: SpuId) -> Option<SpuMetadata<C>> {
        for (_, spu) in self.read().await.iter() {
            if spu.spec.id == id {
                return Some(spu.inner().clone());
            }
        }
        None
    }

    // check if spu can be registered
    async fn validate_spu_for_registered(&self, id: SpuId) -> bool {
        for (_, spu) in self.read().await.iter() {
            if spu.spec.id == id {
                return true;
            }
        }
        false
    }

    async fn all_names(&self) -> Vec<String> {
        self.read().await.keys().cloned().collect()
    }

    async fn table_fmt(&self) -> String {
        let mut table = String::new();

        let hdr = format!(
            "{n:<18}  {d:<6}  {s:<10}  {t:<8}  {p:<16}  {i:<16}  {r}\n",
            n = "SPU",
            d = "SPU-ID",
            s = "STATUS",
            t = "TYPE",
            p = "PUBLIC",
            i = "PRIVATE",
            r = "RACK",
        );
        table.push_str(&hdr);

        for (name, spu) in self.read().await.iter() {
            let rack = match &spu.spec.rack {
                Some(rack) => rack.clone(),
                None => String::from(""),
            };
            let row = format!(
                "{n:<18}  {d:^6}  {s:<10}  {t:<8}  {p:<16}  {i:<16}  {r}\n",
                n = name.clone(),
                d = spu.spec.id,
                s = spu.status.resolution_label(),
                t = spu.spec.spu_type,
                p = spu.spec.public_endpoint,
                i = spu.spec.private_endpoint,
                r = rack,
            );
            table.push_str(&row);
        }

        table
    }

    /// number of spus in rack count
    async fn spus_in_rack_count(&self) -> u32 {
        self.read()
            .await
            .values()
            .filter(|spu| spu.spec.rack.is_some())
            .count() as u32
    }

    // Returns array of touples [("r1", [0,1,2]), ("r2", [3,4]), ("r3", [5])]
    async fn live_spu_rack_map_sorted(&self) -> Vec<(String, Vec<SpuId>)> {
        let rack_map = self.online_spu_rack_map().await;
        let mut racked_vector = Vec::from_iter(rack_map);
        racked_vector.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        racked_vector
    }

    // Return a list of spu ids sorted by rack ["r1":[0,1,2], "r2":[3,4], "r3":[5]]
    async fn online_spu_rack_map(&self) -> BTreeMap<String, Vec<SpuId>> {
        let mut rack_spus: BTreeMap<String, Vec<SpuId>> = BTreeMap::new();

        for spu in self.read().await.values() {
            if let Some(rack) = &spu.spec.rack {
                let mut ids: Vec<SpuId>;
                let mut ids_in_map = rack_spus.remove(rack);
                if ids_in_map.is_some() {
                    ids = ids_in_map.as_mut().unwrap().to_vec();
                    ids.push(spu.spec.id);
                } else {
                    ids = vec![spu.spec.id];
                }
                ids.sort_unstable();
                rack_spus.insert(rack.clone(), ids);
            }
        }

        rack_spus
    }

    // Returns a list of rack inter-leaved spus [0, 4, 5, 1, 3, 2]
    fn online_spus_in_rack(rack_map: &[(String, Vec<SpuId>)]) -> Vec<SpuId> {
        let mut spus = vec![];
        let row_max = rack_map.len();
        let col_max = rack_map.iter().map(|(_, list)| list.len()).max().unwrap();
        let mut row_idx = 0;
        let mut col_idx = 0;

        for idx in 0..(row_max * col_max) {
            let row_list: &Vec<SpuId> = rack_map.get(row_idx).unwrap().1.as_ref();
            let spu_id = row_list[col_idx % row_list.len()];
            let duplicate = spus.iter().find(|&&id| id == spu_id).map(|_| true);
            if duplicate.is_none() {
                spus.push(spu_id);
            }
            row_idx = (row_idx + 1) % row_max;
            col_idx = (((idx + 1) / row_max) + row_idx) % col_max;
        }

        spus
    }

    async fn all_spus_to_spu_msgs(&self) -> Vec<SpuMsg> {
        self.clone_specs()
            .await
            .into_iter()
            .map(SpuMsg::update)
            .collect()
    }

    fn quick(spus: Vec<(SpuId, bool, Option<String>)>) -> Self {
        let elements = spus
            .into_iter()
            .map(|(spu_id, online, rack)| {
                let spu_key = format!("spu-{spu_id}");
                SpuMetadata::quick((spu_key, spu_id, online, rack))
            })
            .collect();
        Self::bulk_new(elements)
    }
}

// -----------------------------------
// Unit Tests
// -----------------------------------

#[cfg(test)]
pub mod test {
    use fluvio_types::SpuId;

    use crate::spu::{SpuSpec, SpuStatus};

    use crate::store::actions::*;
    use super::DefaultSpuMd;
    use super::DefaultSpuStore;
    use super::SpuMd;
    use super::SpuLocalStorePolicy;

    #[fluvio_future::test]
    async fn test_spu_inquiry_online_offline_count() {
        let online_spu = DefaultSpuMd::quick(("spu-0", 0, true, None));
        let offline_spu = DefaultSpuMd::quick(("spu-1", 1, false, None));
        let no_status_spu = DefaultSpuMd::quick(("spu-2", 5001, false, None));

        assert!(online_spu.status.is_online());
        assert!(!offline_spu.status.is_online());
        assert!(!no_status_spu.status.is_online());

        let spus = DefaultSpuStore::bulk_new(vec![online_spu, offline_spu, no_status_spu]);

        assert_eq!(spus.count().await, 3);
        assert_eq!(spus.online_spu_count().await, 1);
    }

    #[test]
    fn test_spu_status_updates_online_offline() {
        let mut test_spu = DefaultSpuMd::quick(("spu", 10, false, None));
        assert_eq!(test_spu.spec.id, 10);

        test_spu.status.set_online();
        assert!(test_spu.status.is_online());

        test_spu.status.set_offline();
        assert!(!test_spu.status.is_online());
    }

    #[fluvio_future::test]
    async fn test_delete_spu_from_local_cache() {
        let online_spu = DefaultSpuMd::quick(("spu-0", 0, true, None));
        let offline_spu = DefaultSpuMd::quick(("spu-1", 1, false, None));

        let spus = DefaultSpuStore::bulk_new(vec![online_spu, offline_spu]);

        assert_eq!(spus.online_spu_count().await, 1);
        assert_eq!(spus.count().await, 2);

        assert_eq!(spus.epoch().await, 0);

        let status = spus
            .apply_changes(vec![LSUpdate::Delete("spu-0".to_owned())])
            .await
            .expect("some");
        assert_eq!(status.add, 0);
        assert_eq!(status.update_spec, 0);
        assert_eq!(status.delete, 1);
        assert_eq!(status.epoch, 1);
        assert_eq!(spus.online_spu_count().await, 0);
        assert_eq!(spus.count().await, 1);
        assert_eq!(spus.epoch().await, 1);
    }

    #[fluvio_future::test]
    async fn test_update_spu_spec_in_local_cache() {
        let spu_0 = DefaultSpuMd::quick(("spu-0", 0, false, None));
        let mut spu_1 = DefaultSpuMd::quick(("spu-1", 1, false, None));

        let other_spec = SpuSpec {
            id: 1,
            rack: Some("rack".to_string()),
            ..Default::default()
        };
        let other_spu = DefaultSpuMd::new("spu-1", other_spec.clone(), SpuStatus::default());

        let spus = DefaultSpuStore::bulk_new(vec![spu_0, spu_1.clone()]);

        // update spec on spu_1
        spu_1.spec = other_spec.clone();
        let status = spus
            .apply_changes(vec![LSUpdate::Mod(spu_1)])
            .await
            .expect("some");

        assert_eq!(status.add, 0);
        assert_eq!(status.update_spec, 1);
        assert_eq!(status.delete, 0);

        // test result
        let updated_spu = spus.value("spu-1").await.expect("lookup");
        assert_eq!(updated_spu.inner_owned(), other_spu);
    }

    #[fluvio_future::test]
    async fn test_update_spu_status_in_local_cache() {
        let online = DefaultSpuMd::quick(("spu-0", 0, true, None));
        let offline = DefaultSpuMd::quick(("spu-1", 1, false, None));
        let offline2 = DefaultSpuMd::quick(("spu-3", 2, false, None));

        assert!(online.status.is_online());
        assert!(!offline.status.is_online());

        let spus =
            DefaultSpuStore::bulk_new(vec![online.clone(), offline.clone(), offline2.clone()]);
        assert_eq!(spus.count().await, 3);
        assert_eq!(spus.online_spu_count().await, 1);

        // [online] -> [offline] for spu0
        let mut online_update = online.clone();
        online_update.status = offline.status.clone();
        let status = spus
            .apply_changes(vec![LSUpdate::Mod(online_update)])
            .await
            .expect("some");
        let spu = spus.value("spu-0").await.expect("spu");
        assert_eq!(status.add, 0);
        assert_eq!(status.update_status, 1);
        assert_eq!(status.update_spec, 0);
        assert_eq!(status.delete, 0);
        assert_eq!(spus.count().await, 3);
        assert_eq!(spus.online_spu_count().await, 0);
        assert!(!spu.status.is_online());

        // [offline] -> [online]
        let mut offline_update = offline2.clone();
        offline_update.status = online.status.clone();
        let status = spus
            .apply_changes(vec![LSUpdate::Mod(offline_update)])
            .await
            .expect("some");
        assert_eq!(status.add, 0);
        assert_eq!(status.update_status, 1);
        assert_eq!(status.delete, 0);
        let spu = spus.value("spu-3").await.expect("spu");
        assert_eq!(spus.count().await, 3);
        assert_eq!(spus.online_spu_count().await, 1);
        assert!(spu.status.is_online());
    }

    #[fluvio_future::test]
    async fn rack_map_test_racks_3_spus_6_unbalanced() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");

        let spus = DefaultSpuStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r2.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r3.clone())),
        ]);

        // run test
        let rack_map = DefaultSpuStore::live_spu_rack_map_sorted(&spus).await;
        let spu_list = DefaultSpuStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<SpuId>)> = vec![
            (r1.clone(), vec![0, 1, 2]),
            (r2.clone(), vec![3, 4]),
            (r3.clone(), vec![5]),
        ];
        let expected_list: Vec<SpuId> = vec![0, 4, 5, 1, 3, 2];

        assert_eq!(6, spus.count().await);
        assert_eq!(6, spus.online_spu_count().await);
        assert_eq!(expected_map, rack_map);
        assert_eq!(expected_list, spu_list);
    }

    #[fluvio_future::test]
    async fn rack_map_test_racks_5_spus_10_unbalanced() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");
        let r5 = String::from("r5");

        let spus = DefaultSpuStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r1.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r2.clone())),
            (6, true, Some(r3.clone())),
            (7, true, Some(r3.clone())),
            (8, true, Some(r4.clone())),
            (9, true, Some(r5.clone())),
        ]);

        // run test
        let rack_map = DefaultSpuStore::live_spu_rack_map_sorted(&spus).await;
        let spu_list = DefaultSpuStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<SpuId>)> = vec![
            (r1.clone(), vec![0, 1, 2, 3]),
            (r2.clone(), vec![4, 5]),
            (r3.clone(), vec![6, 7]),
            (r4.clone(), vec![8]),
            (r5.clone(), vec![9]),
        ];
        let expected_list: Vec<SpuId> = vec![0, 5, 6, 8, 9, 1, 4, 7, 2, 3];

        assert_eq!(rack_map, expected_map);
        assert_eq!(spu_list, expected_list);
    }

    #[fluvio_future::test]
    async fn rack_map_test_racks_4_spus_10_unbalanced() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");

        let spus = DefaultSpuStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r2.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r2.clone())),
            (6, true, Some(r3.clone())),
            (7, true, Some(r3.clone())),
            (8, true, Some(r4.clone())),
            (9, true, Some(r4.clone())),
        ]);

        // run test
        let rack_map = DefaultSpuStore::live_spu_rack_map_sorted(&spus).await;
        let spu_list = DefaultSpuStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<SpuId>)> = vec![
            (String::from("r1"), vec![0, 1, 2]),
            (String::from("r2"), vec![3, 4, 5]),
            (String::from("r3"), vec![6, 7]),
            (String::from("r4"), vec![8, 9]),
        ];
        let expected_list: Vec<SpuId> = vec![0, 4, 6, 8, 1, 5, 9, 2, 3, 7];

        assert_eq!(rack_map, expected_map);
        assert_eq!(spu_list, expected_list);
    }

    #[fluvio_future::test]
    async fn rack_map_test_racks_4_spus_12_full() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");

        let spus = DefaultSpuStore::quick(vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r2.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r2.clone())),
            (6, true, Some(r3.clone())),
            (7, true, Some(r3.clone())),
            (8, true, Some(r3.clone())),
            (9, true, Some(r4.clone())),
            (10, true, Some(r4.clone())),
            (11, true, Some(r4.clone())),
        ]);

        // run test
        let rack_map = DefaultSpuStore::live_spu_rack_map_sorted(&spus).await;
        let spu_list = DefaultSpuStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<SpuId>)> = vec![
            (String::from("r1"), vec![0, 1, 2]),
            (String::from("r2"), vec![3, 4, 5]),
            (String::from("r3"), vec![6, 7, 8]),
            (String::from("r4"), vec![9, 10, 11]),
        ];
        let expected_list: Vec<SpuId> = vec![0, 4, 8, 9, 1, 5, 6, 10, 2, 3, 7, 11];

        assert_eq!(rack_map, expected_map);
        assert_eq!(spu_list, expected_list);
    }
}
