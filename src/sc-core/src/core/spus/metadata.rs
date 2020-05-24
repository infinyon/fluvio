//!
//! # SC Spu Metadata
//!
//! Spu metadata information cached locally.
//!
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::io::Error as IoError;
use std::io::ErrorKind;

use flv_types::socket_helpers::ServerAddress;
use flv_types::SpuId;
use flv_metadata::spu::{Endpoint, SpuSpec, SpuStatus, IngressPort};
use k8_metadata::metadata::K8Obj;
use k8_metadata::spu::SpuSpec as K8SpuSpec;
use internal_api::messages::SpuMsg;

use crate::core::common::LocalStore;
use crate::core::common::KVObject;
use crate::core::Spec;
use crate::core::Status;
use crate::metadata::default_convert_from_k8;

impl Spec for SpuSpec {
    const LABEL: &'static str = "SPU";
    type Key = String;
    type Status = SpuStatus;
    type K8Spec = K8SpuSpec;
    type Owner = SpuSpec;

    fn convert_from_k8(
        k8_obj: K8Obj<Self::K8Spec>,
    ) -> Result<KVObject<Self>, IoError> {
        default_convert_from_k8(k8_obj)
    }
}

impl Status for SpuStatus {}

// -----------------------------------
// Data Structures
// -----------------------------------
pub type SpuKV = KVObject<SpuSpec>;

// -----------------------------------
// Spu - Implementation
// -----------------------------------

impl SpuKV {
    //
    // Accessors
    //
    pub fn id(&self) -> &SpuId {
        &self.spec.id
    }

    pub fn name(&self) -> &String {
        &self.key
    }

    pub fn rack(&self) -> Option<&String> {
        match &self.spec.rack {
            Some(rack) => Some(rack),
            None => None,
        }
    }

    pub fn rack_clone(&self) -> Option<String> {
        match &self.spec.rack {
            Some(rack) => Some(rack.clone()),
            None => None,
        }
    }

    pub fn public_endpoint(&self) -> &IngressPort {
        &self.spec.public_endpoint
    }

    pub fn private_endpoint(&self) -> &Endpoint {
        &self.spec.private_endpoint
    }

    pub fn resolution_label(&self) -> &'static str {
        self.status.resolution_label()
    }

    pub fn type_label(&self) -> String {
        self.spec.type_label()
    }

    pub fn is_custom(&self) -> bool {
        self.spec.is_custom()
    }

    pub fn is_managed(&self) -> bool {
        !self.spec.is_custom()
    }

    pub fn private_server_address(&self) -> ServerAddress {
        let private_ep = self.private_endpoint();
        ServerAddress {
            host: private_ep.host.clone(),
            port: private_ep.port,
        }
    }

    pub fn set_rack(&mut self, rack: Option<&String>) {
        match rack {
            Some(r) => self.spec.rack = Some(r.clone()),
            None => self.spec.rack = None,
        }
    }

    pub fn set_public_endpoint(&mut self, public_ep: IngressPort) {
        self.spec.public_endpoint = public_ep;
    }

    pub fn set_private_endpoint(&mut self, private_ep: Endpoint) {
        self.spec.private_endpoint = private_ep;
    }
}

/// used in the bulk add scenario
impl<J> From<(J, i32, bool, Option<String>)> for SpuKV
where
    J: Into<String>,
{
    fn from(spu: (J, i32, bool, Option<String>)) -> Self {
        let mut spec = SpuSpec::default();
        spec.id = spu.1;
        spec.rack = spu.3;

        let mut status = SpuStatus::default();
        if spu.2 {
            status.set_online();
        }

        Self::new(spu.0.into(), spec, status)
    }
}

pub type SpuLocalStore = LocalStore<SpuSpec>;

// -----------------------------------
// Spus - Implementation
// -----------------------------------

impl SpuLocalStore {
    /// update the spec
    pub fn update_spec(&self, name: &str, other_spu: &SpuKV) -> Result<(), IoError> {
        if let Some(spu) = (*self.inner_store().write()).get_mut(name) {
            if spu.id() != other_spu.id() {
                Err(IoError::new(
                    ErrorKind::InvalidData,
                    format!("spu '{}': id is immutable", name),
                ))
            } else {
                if spu.rack() != other_spu.rack() {
                    spu.set_rack(other_spu.rack());
                }
                if spu.public_endpoint() != other_spu.public_endpoint() {
                    spu.set_public_endpoint(other_spu.public_endpoint().clone());
                }
                if spu.private_endpoint() != other_spu.private_endpoint() {
                    spu.set_private_endpoint(other_spu.private_endpoint().clone());
                }
                spu.set_ctx(other_spu.kv_ctx());

                Ok(())
            }
        } else {
            Err(IoError::new(
                ErrorKind::InvalidData,
                format!("spu '{}': not found, cannot update", name),
            ))
        }
    }

    // do bulk add,
    // assume spu stars with id
    #[cfg(test)]
    pub fn bulk_add(&self, spus: Vec<(i32, bool, Option<String>)>) {
        for (spu_id, online, rack) in spus.into_iter() {
            let spu_key = format!("spu-{}", spu_id);
            let spu: SpuKV = (spu_key, spu_id, online, rack).into();
            self.insert(spu);
        }
    }

    // build hashmap of online
    pub fn online_status(&self) -> HashSet<SpuId> {
        let mut status = HashSet::new();
        for (_, spu) in self.inner_store().read().iter() {
            if spu.status.is_online() {
                status.insert(*spu.id());
            }
        }
        status
    }

    /// count online SPUs
    pub fn online_spu_count(&self) -> i32 {
        self.inner_store()
            .read()
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
    pub fn spu_used_for_replica(&self) -> i32 {
        self.count()
    }

    // retrieve SPU ids.
    pub fn online_spu_ids(&self) -> Vec<i32> {
        self.inner_store()
            .read()
            .values()
            .filter_map(|spu| {
                if spu.status.is_online() {
                    Some(*spu.id())
                } else {
                    None
                }
            })
            .collect()
    }

    // find spu id that can be used in the reeokuca
    pub fn spu_ids_for_replica(&self) -> Vec<i32> {
        self.inner_store()
            .read()
            .values()
            .filter_map(|spu| Some(*spu.id()))
            .collect()
    }

    pub fn online_spus(&self) -> Vec<SpuKV> {
        self.inner_store()
            .read()
            .values()
            .filter_map(|spu| {
                if spu.status.is_online() {
                    Some(spu.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn custom_spus(&self) -> Vec<SpuKV> {
        self.inner_store()
            .read()
            .values()
            .filter_map(|spu| {
                if spu.is_custom() {
                    Some(spu.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn spu(&self, name: &str) -> Option<SpuKV> {
        match (*self.inner_store().read()).get(name) {
            Some(spu) => Some(spu.clone()),
            None => None,
        }
    }

    pub fn get_by_id(&self, id: &i32) -> Option<SpuKV> {
        for (_, spu) in (*self.inner_store().read()).iter() {
            if spu.id() == id {
                return Some(spu.clone());
            }
        }
        None
    }

    // check if spu can be registered
    pub fn validate_spu_for_registered(&self, id: &SpuId) -> bool {
        for (_, spu) in (self.inner_store().read()).iter() {
            if spu.id() == id && spu.status.is_offline() {
                return true;
            }
        }
        false
    }

    // check if given range is conflict with any of the range
    pub fn is_conflict(&self, owner_uid: &str, start: i32, end_exclusive: i32) -> Option<i32> {
        for (_, spu) in (self.inner_store().read()).iter() {
            if !spu.is_owned(owner_uid) {
                let id = *spu.id();
                if id >= start && id < end_exclusive {
                    return Some(id);
                }
            }
        }
        None
    }

    #[cfg(test)]
    pub fn all_spu_count(&self) -> i32 {
        self.inner_store().read().len() as i32
    }

    pub fn all_names(&self) -> Vec<String> {
        self.inner_store().read().keys().cloned().collect()
    }

    pub fn table_fmt(&self) -> String {
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

        for (name, spu) in self.inner_store().read().iter() {
            let rack = match spu.rack() {
                Some(rack) => rack.clone(),
                None => String::from(""),
            };
            let row = format!(
                "{n:<18}  {d:^6}  {s:<10}  {t:<8}  {p:<16}  {i:<16}  {r}\n",
                n = name.clone(),
                d = spu.id(),
                s = spu.resolution_label(),
                t = spu.type_label().clone(),
                p = spu.public_endpoint(),
                i = spu.private_endpoint(),
                r = rack,
            );
            table.push_str(&row);
        }

        table
    }

    /// number of spus in rack count
    pub fn spus_in_rack_count(&self) -> i32 {
        self.inner_store()
            .read()
            .values()
            .filter_map(|spu| if spu.rack().is_some() { Some(1) } else { None })
            .sum()
    }

    // Returns array of touples [("r1", [0,1,2]), ("r2", [3,4]), ("r3", [5])]
    pub fn live_spu_rack_map_sorted(spus: &SpuLocalStore) -> Vec<(String, Vec<i32>)> {
        let rack_map = SpuLocalStore::online_spu_rack_map(spus);
        let mut racked_vector = Vec::from_iter(rack_map);
        racked_vector.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        racked_vector
    }

    // Return a list of spu ids sorted by rack ["r1":[0,1,2], "r2":[3,4], "r3":[5]]
    fn online_spu_rack_map(spus: &SpuLocalStore) -> BTreeMap<String, Vec<i32>> {
        let mut rack_spus: BTreeMap<String, Vec<i32>> = BTreeMap::new();

        for spu in spus.inner_store().read().values() {
            if let Some(rack) = spu.rack() {
                let mut ids: Vec<i32>;
                let mut ids_in_map = rack_spus.remove(rack);
                if ids_in_map.is_some() {
                    ids = ids_in_map.as_mut().unwrap().to_vec();
                    ids.push(*spu.id());
                } else {
                    ids = vec![*spu.id()];
                }
                ids.sort();
                rack_spus.insert(rack.clone(), ids);
            }
        }

        rack_spus
    }

    // Returns a list of rack inter-leaved spus [0, 4, 5, 1, 3, 2]
    pub fn online_spus_in_rack(rack_map: &Vec<(String, Vec<i32>)>) -> Vec<i32> {
        let mut spus = vec![];
        let row_max = rack_map.len();
        let col_max = rack_map.iter().map(|(_, list)| list.len()).max().unwrap();
        let mut row_idx = 0;
        let mut col_idx = 0;

        for idx in 0..(row_max * col_max) {
            let row_list: &Vec<i32> = rack_map.get(row_idx).unwrap().1.as_ref();
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

    /// Encode all online SPUs to SPU Messages
    pub fn all_spus_to_spu_msgs(&self) -> Vec<SpuMsg> {
        self.all_specs()
            .into_iter()
            .map(|spu_spec| SpuMsg::update(spu_spec.into()))
            .collect()
    }
}

#[cfg(test)]
impl From<Vec<(i32, bool, Option<String>)>> for SpuLocalStore {
    fn from(spus: Vec<(i32, bool, Option<String>)>) -> Self {
        let store = Self::default();
        store.bulk_add(spus);
        store
    }
}

// -----------------------------------
// Unit Tests
// -----------------------------------

#[cfg(test)]
pub mod test {
    use flv_metadata::spu::{SpuSpec, SpuStatus};

    use super::{SpuKV, SpuLocalStore};

    #[test]
    fn test_spu_inquiry_online_offline_count() {
        let online_spu: SpuKV = ("spu-0", 0, true, None).into();
        let offline_spu: SpuKV = ("spu-1", 1, false, None).into();
        let no_status_spu: SpuKV = ("spu-2", 5001, false, None).into();

        assert_eq!(online_spu.status.is_online(), true);
        assert_eq!(offline_spu.status.is_online(), false);
        assert_eq!(no_status_spu.status.is_online(), false);

        let spus = SpuLocalStore::default();
        spus.insert(online_spu);
        spus.insert(offline_spu);
        spus.insert(no_status_spu);

        assert_eq!(spus.all_spu_count(), 3);
        assert_eq!(spus.online_spu_count(), 1);
    }

    #[test]
    fn test_spu_status_updates_online_offline() {
        let mut test_spu: SpuKV = ("spu", 10, false, None).into();
        assert_eq!(*test_spu.id(), 10);

        test_spu.status.set_online();
        assert_eq!(test_spu.status.is_online(), true);

        test_spu.status.set_offline();
        assert_eq!(test_spu.status.is_online(), false);
    }

    #[test]
    fn test_delete_spu_from_local_cache() {
        let online_spu: SpuKV = ("spu-0", 0, true, None).into();
        let offline_spu: SpuKV = ("spu-1", 1, false, None).into();

        let spus = SpuLocalStore::default();
        spus.insert(online_spu);
        spus.insert(offline_spu);

        assert_eq!(spus.online_spu_count(), 1);
        assert_eq!(spus.all_spu_count(), 2);

        spus.delete("spu-0");

        assert_eq!(spus.online_spu_count(), 0);
        assert_eq!(spus.all_spu_count(), 1);
    }

    #[test]
    fn test_update_spu_spec_in_local_cache() {
        let spu_0 = ("spu-0", 0, false, None).into();
        let spu_1 = ("spu-1", 1, false, None).into();

        let mut other_spec = SpuSpec::default();
        other_spec.id = 1;
        other_spec.rack = Some("rack".to_string());
        let other_spu = SpuKV::new("spu-1", other_spec, SpuStatus::default());

        let spus = SpuLocalStore::default();
        spus.insert(spu_0);
        spus.insert(spu_1);

        // run test
        let res = spus.update_spec("spu-1", &other_spu);
        assert_eq!(res.is_ok(), true);

        // test result
        let updated_spu = spus.spu("spu-1").unwrap();
        assert_eq!(updated_spu, other_spu);
    }

    #[test]
    fn test_update_spu_status_in_local_cache() {
        let online: SpuKV = ("spu-0", 0, true, None).into();
        let offline: SpuKV = ("spu-1", 1, false, None).into();
        let offline2: SpuKV = ("spu-3", 2, false, None).into();

        assert_eq!(online.status.is_online(), true);
        assert_eq!(offline.status.is_online(), false);

        let spus = SpuLocalStore::default();
        spus.insert(online.clone());
        spus.insert(offline.clone());
        spus.insert(offline2);
        assert_eq!(spus.all_spu_count(), 3);
        assert_eq!(spus.online_spu_count(), 1);

        //test - not found
        let res = spus.update_status("spu-9", offline.status.clone());
        assert_eq!(
            res.unwrap_err().to_string(),
            "SPU 'spu-9': not found, cannot update"
        );

        // [online] -> [offline]
        let res = spus.update_status("spu-0", offline.status.clone());
        let spu = spus.spu("spu-0");
        assert_eq!(res.is_ok(), true);
        assert_eq!(spus.all_spu_count(), 3);
        assert_eq!(spus.online_spu_count(), 0);
        assert_eq!(spu.unwrap().status.is_online(), false);

        // [offline] -> [online]
        let res = spus.update_status("spu-3", online.status.clone());
        let spu = spus.spu("spu-3");
        assert_eq!(res.is_ok(), true);
        assert_eq!(spus.all_spu_count(), 3);
        assert_eq!(spus.online_spu_count(), 1);
        assert_eq!(spu.unwrap().status.is_online(), true);
    }

    #[test]
    fn rack_map_test_racks_3_spus_6_unbalanced() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");

        let spus = vec![
            (0, true, Some(r1.clone())),
            (1, true, Some(r1.clone())),
            (2, true, Some(r1.clone())),
            (3, true, Some(r2.clone())),
            (4, true, Some(r2.clone())),
            (5, true, Some(r3.clone())),
        ]
        .into();

        // run test
        let rack_map = SpuLocalStore::live_spu_rack_map_sorted(&spus);
        let spu_list = SpuLocalStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<i32>)> = vec![
            (r1.clone(), vec![0, 1, 2]),
            (r2.clone(), vec![3, 4]),
            (r3.clone(), vec![5]),
        ];
        let expected_list = vec![0, 4, 5, 1, 3, 2];

        assert_eq!(6, spus.all_spu_count());
        assert_eq!(6, spus.online_spu_count());
        assert_eq!(expected_map, rack_map);
        assert_eq!(expected_list, spu_list);
    }

    #[test]
    fn rack_map_test_racks_5_spus_10_unbalanced() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");
        let r5 = String::from("r5");

        let spus = vec![
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
        ]
        .into();

        // run test
        let rack_map = SpuLocalStore::live_spu_rack_map_sorted(&spus);
        let spu_list = SpuLocalStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<i32>)> = vec![
            (r1.clone(), vec![0, 1, 2, 3]),
            (r2.clone(), vec![4, 5]),
            (r3.clone(), vec![6, 7]),
            (r4.clone(), vec![8]),
            (r5.clone(), vec![9]),
        ];
        let expected_list = vec![0, 5, 6, 8, 9, 1, 4, 7, 2, 3];

        assert_eq!(rack_map, expected_map);
        assert_eq!(spu_list, expected_list);
    }

    #[test]
    fn rack_map_test_racks_4_spus_10_unbalanced() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");

        let spus = vec![
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
        ]
        .into();

        // run test
        let rack_map = SpuLocalStore::live_spu_rack_map_sorted(&spus);
        let spu_list = SpuLocalStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<i32>)> = vec![
            (String::from("r1"), vec![0, 1, 2]),
            (String::from("r2"), vec![3, 4, 5]),
            (String::from("r3"), vec![6, 7]),
            (String::from("r4"), vec![8, 9]),
        ];
        let expected_list = vec![0, 4, 6, 8, 1, 5, 9, 2, 3, 7];

        assert_eq!(rack_map, expected_map);
        assert_eq!(spu_list, expected_list);
    }

    #[test]
    fn rack_map_test_racks_4_spus_12_full() {
        let r1 = String::from("r1");
        let r2 = String::from("r2");
        let r3 = String::from("r3");
        let r4 = String::from("r4");

        let spus = vec![
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
        ]
        .into();

        // run test
        let rack_map = SpuLocalStore::live_spu_rack_map_sorted(&spus);
        let spu_list = SpuLocalStore::online_spus_in_rack(&rack_map);

        // validate result
        let expected_map: Vec<(String, Vec<i32>)> = vec![
            (String::from("r1"), vec![0, 1, 2]),
            (String::from("r2"), vec![3, 4, 5]),
            (String::from("r3"), vec![6, 7, 8]),
            (String::from("r4"), vec![9, 10, 11]),
        ];
        let expected_list = vec![0, 4, 8, 9, 1, 5, 6, 10, 2, 3, 7, 11];

        assert_eq!(rack_map, expected_map);
        assert_eq!(spu_list, expected_list);
    }
}
