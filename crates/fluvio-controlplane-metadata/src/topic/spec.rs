#![allow(clippy::assign_op_pattern)]

//!
//! # Topic Spec
//!
//! Topic spec consists of 2 types of topics
//!  * Assigned
//!  * Computed
//!
use std::io::{Error, ErrorKind};
use std::collections::BTreeMap;
use std::ops::Deref;

use tracing::{trace};

use fluvio_types::defaults::{
    STORAGE_RETENTION_SECONDS, SPU_LOG_LOG_SEGMENT_MAX_BYTE_MIN, STORAGE_RETENTION_SECONDS_MIN,
    SPU_PARTITION_MAX_BYTES_MIN, SPU_LOG_SEGMENT_MAX_BYTES,
};
use fluvio_types::{ReplicaMap, SpuId};
use fluvio_types::{PartitionId, PartitionCount, ReplicationFactor, IgnoreRackAssignment};

use fluvio_protocol::Version;
use fluvio_protocol::bytes::{Buf, BufMut};
use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Clone, PartialEq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TopicSpec {
    replicas: ReplicaSpec,
    #[fluvio(min_version = 3)]
    cleanup_policy: Option<CleanupPolicy>,
    #[fluvio(min_version = 4)]
    storage: Option<TopicStorageConfig>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    #[fluvio(min_version = 6)]
    compression_type: CompressionAlgorithm,
}

impl From<ReplicaSpec> for TopicSpec {
    fn from(replicas: ReplicaSpec) -> Self {
        Self {
            replicas,
            ..Default::default()
        }
    }
}

impl Deref for TopicSpec {
    type Target = ReplicaSpec;

    fn deref(&self) -> &Self::Target {
        &self.replicas
    }
}

impl TopicSpec {
    pub fn new_assigned<J>(partition_map: J) -> Self
    where
        J: Into<PartitionMaps>,
    {
        Self {
            replicas: ReplicaSpec::new_assigned(partition_map),
            ..Default::default()
        }
    }

    pub fn new_computed(
        partitions: PartitionCount,
        replication: ReplicationFactor,
        ignore_rack: Option<IgnoreRackAssignment>,
    ) -> Self {
        Self {
            replicas: ReplicaSpec::new_computed(partitions, replication, ignore_rack),
            ..Default::default()
        }
    }

    #[inline(always)]
    pub fn replicas(&self) -> &ReplicaSpec {
        &self.replicas
    }

    pub fn set_cleanup_policy(&mut self, policy: CleanupPolicy) {
        self.cleanup_policy = Some(policy);
    }

    pub fn get_clean_policy(&self) -> Option<&CleanupPolicy> {
        self.cleanup_policy.as_ref()
    }

    pub fn set_compression_type(&mut self, compression: CompressionAlgorithm) {
        self.compression_type = compression;
    }

    pub fn get_compression_type(&self) -> &CompressionAlgorithm {
        &self.compression_type
    }

    pub fn get_storage(&self) -> Option<&TopicStorageConfig> {
        self.storage.as_ref()
    }

    pub fn get_storage_mut(&mut self) -> Option<&mut TopicStorageConfig> {
        self.storage.as_mut()
    }

    pub fn set_storage(&mut self, storage: TopicStorageConfig) {
        self.storage = Some(storage);
    }

    /// get retention secs that can be displayed
    pub fn retention_secs(&self) -> u32 {
        self.get_clean_policy()
            .map(|policy| policy.retention_secs())
            .unwrap_or_else(|| STORAGE_RETENTION_SECONDS)
    }

    /// validate configuration, return string with errors
    pub fn validate_config(&self) -> Option<String> {
        if let Some(policy) = self.get_clean_policy() {
            if policy.retention_secs() < STORAGE_RETENTION_SECONDS_MIN {
                return Some(format!(
                    "retention_secs {} is less than minimum {}",
                    policy.retention_secs(),
                    STORAGE_RETENTION_SECONDS_MIN
                ));
            }
        }

        if let Some(storage) = self.get_storage() {
            if let Some(segment_size) = storage.segment_size {
                if segment_size < SPU_LOG_LOG_SEGMENT_MAX_BYTE_MIN {
                    return Some(format!(
                        "segment_size {segment_size} is less than minimum {SPU_LOG_LOG_SEGMENT_MAX_BYTE_MIN}"
                    ));
                }
            }
            if let Some(max_partition_size) = storage.max_partition_size {
                if max_partition_size < SPU_PARTITION_MAX_BYTES_MIN {
                    return Some(format!(
                        "max_partition_size {max_partition_size} is less than minimum {SPU_PARTITION_MAX_BYTES_MIN}"
                    ));
                }
                let segment_size = storage.segment_size.unwrap_or(SPU_LOG_SEGMENT_MAX_BYTES);
                if max_partition_size < segment_size as u64 {
                    return Some(format!(
                        "max_partition_size {max_partition_size} is less than segment size {segment_size}"
                    ));
                }
            }
        }

        None
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ReplicaSpec {
    #[cfg_attr(feature = "use_serde", serde(rename = "assigned"))]
    Assigned(PartitionMaps),
    #[cfg_attr(feature = "use_serde", serde(rename = "computed"))]
    Computed(TopicReplicaParam),
}

impl std::fmt::Display for ReplicaSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Assigned(partition_map) => write!(f, "assigned::{partition_map}"),
            Self::Computed(param) => write!(f, "computed::({param})"),
        }
    }
}

impl Default for ReplicaSpec {
    fn default() -> Self {
        Self::Assigned(PartitionMaps::default())
    }
}

impl ReplicaSpec {
    pub fn new_assigned<J>(partition_map: J) -> Self
    where
        J: Into<PartitionMaps>,
    {
        Self::Assigned(partition_map.into())
    }

    pub fn new_computed(
        partitions: PartitionCount,
        replication: ReplicationFactor,
        ignore_rack: Option<IgnoreRackAssignment>,
    ) -> Self {
        Self::Computed((partitions, replication, ignore_rack.unwrap_or(false)).into())
    }

    pub fn is_computed(&self) -> bool {
        match self {
            Self::Computed(_) => true,
            Self::Assigned(_) => false,
        }
    }

    pub fn partitions(&self) -> PartitionCount {
        match &self {
            Self::Computed(param) => param.partitions,
            Self::Assigned(partition_map) => partition_map.partition_count(),
        }
    }

    pub fn replication_factor(&self) -> Option<ReplicationFactor> {
        match self {
            Self::Computed(param) => Some(param.replication_factor),
            Self::Assigned(partition_map) => partition_map.replication_factor(),
        }
    }

    pub fn ignore_rack_assignment(&self) -> IgnoreRackAssignment {
        match self {
            Self::Computed(param) => param.ignore_rack_assignment,
            Self::Assigned(_) => false,
        }
    }

    pub fn type_label(&self) -> &'static str {
        match self {
            Self::Computed(_) => "computed",
            Self::Assigned(_) => "assigned",
        }
    }

    pub fn partitions_display(&self) -> String {
        match self {
            Self::Computed(param) => param.partitions.to_string(),
            Self::Assigned(_) => "".to_owned(),
        }
    }

    pub fn replication_factor_display(&self) -> String {
        match self {
            Self::Computed(param) => param.replication_factor.to_string(),
            Self::Assigned(_) => "".to_owned(),
        }
    }

    pub fn ignore_rack_assign_display(&self) -> &'static str {
        match self {
            Self::Computed(param) => {
                if param.ignore_rack_assignment {
                    "yes"
                } else {
                    ""
                }
            }
            Self::Assigned(_) => "",
        }
    }

    pub fn partition_map_str(&self) -> Option<String> {
        match self {
            Self::Computed(_) => None,
            Self::Assigned(partition_map) => Some(partition_map.partition_map_string()),
        }
    }

    // -----------------------------------
    //  Parameter validation
    // -----------------------------------

    /// Validate partitions
    pub fn valid_partition(partitions: &PartitionCount) -> Result<(), Error> {
        if *partitions == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "partition must be greater than 0",
            ));
        }

        Ok(())
    }

    /// Validate replication factor
    pub fn valid_replication_factor(replication: &ReplicationFactor) -> Result<(), Error> {
        if *replication == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "replication factor must be greater than 0",
            ));
        }

        Ok(())
    }
}

impl Decoder for ReplicaSpec {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut typ: u8 = 0;
        typ.decode(src, version)?;
        trace!("decoded type: {}", typ);

        match typ {
            // Assigned Replicas
            0 => {
                let mut partition_map = PartitionMaps::default();
                partition_map.decode(src, version)?;
                *self = Self::Assigned(partition_map);
                Ok(())
            }

            // Computed Replicas
            1 => {
                let mut param = TopicReplicaParam::default();
                param.decode(src, version)?;
                *self = Self::Computed(param);
                Ok(())
            }

            // Unexpected type
            _ => Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("unknown replica type {typ}"),
            )),
        }
    }
}

// -----------------------------------
// Encoder / Decoder
// -----------------------------------
impl Encoder for ReplicaSpec {
    // compute size for fluvio replicas
    fn write_size(&self, version: Version) -> usize {
        let typ_size = (0u8).write_size(version);
        match self {
            Self::Assigned(partitions) => typ_size + partitions.write_size(version),
            Self::Computed(param) => typ_size + param.write_size(version),
        }
    }

    // encode fluvio replicas
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for replica len of {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            // encode assign partitions
            Self::Assigned(partitions) => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
                partitions.encode(dest, version)?;
            }

            // encode computed partitions
            Self::Computed(param) => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
                param.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

/// Topic param
#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TopicReplicaParam {
    #[cfg_attr(feature = "use_serde", serde(default = "default_count"))]
    pub partitions: PartitionCount,
    #[cfg_attr(feature = "use_serde", serde(default = "default_count"))]
    pub replication_factor: ReplicationFactor,
    #[cfg_attr(
        feature = "use_serde",
        serde(skip_serializing_if = "bool::clone", default)
    )]
    pub ignore_rack_assignment: IgnoreRackAssignment,
}

#[allow(dead_code)]
fn default_count() -> u32 {
    1
}

impl TopicReplicaParam {
    pub fn new(
        partitions: PartitionCount,
        replication_factor: ReplicationFactor,
        ignore_rack_assignment: IgnoreRackAssignment,
    ) -> Self {
        Self {
            partitions,
            replication_factor,
            ignore_rack_assignment,
        }
    }
}

impl From<(PartitionCount, ReplicationFactor, IgnoreRackAssignment)> for TopicReplicaParam {
    fn from(value: (PartitionCount, ReplicationFactor, IgnoreRackAssignment)) -> Self {
        let (partitions, replication_factor, ignore_rack) = value;
        Self::new(partitions, replication_factor, ignore_rack)
    }
}

impl std::fmt::Display for TopicReplicaParam {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "replica param::(p:{}, r:{})",
            self.partitions, self.replication_factor
        )
    }
}

/// Hack: field instead of new type to get around encode and decode limitations
#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionMaps {
    maps: Vec<PartitionMap>,
}

impl From<Vec<PartitionMap>> for PartitionMaps {
    fn from(maps: Vec<PartitionMap>) -> Self {
        Self { maps }
    }
}

impl From<Vec<(PartitionId, Vec<SpuId>)>> for PartitionMaps {
    fn from(partition_vec: Vec<(PartitionId, Vec<SpuId>)>) -> Self {
        let maps: Vec<PartitionMap> = partition_vec
            .into_iter()
            .map(|(id, replicas)| PartitionMap { id, replicas })
            .collect();
        maps.into()
    }
}

impl std::fmt::Display for PartitionMaps {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "partition map:{})", self.maps.len())
    }
}

impl PartitionMaps {
    pub fn maps(&self) -> &Vec<PartitionMap> {
        &self.maps
    }

    pub fn maps_owned(self) -> Vec<PartitionMap> {
        self.maps
    }

    fn partition_count(&self) -> PartitionCount {
        self.maps.len() as PartitionCount
    }

    fn replication_factor(&self) -> Option<ReplicationFactor> {
        // compute replication form replica map
        self.maps
            .first()
            .map(|partition| partition.replicas.len() as ReplicationFactor)
    }

    fn partition_map_string(&self) -> String {
        use std::fmt::Write;

        let mut res = String::new();
        for partition in &self.maps {
            write!(res, "{}:{:?}, ", partition.id, partition.replicas).unwrap();
            // ok to unwrap since this will not fail
        }
        if !res.is_empty() {
            res.truncate(res.len() - 2);
        }
        res
    }

    // -----------------------------------
    // Partition Map - Operations
    // -----------------------------------

    /// Generate a vector with all spu ids represented by all partitions (no duplicates)
    pub fn unique_spus_in_partition_map(&self) -> Vec<SpuId> {
        let mut spu_ids: Vec<SpuId> = vec![];

        for partition in &self.maps {
            for spu in &partition.replicas {
                if !spu_ids.contains(spu) {
                    spu_ids.push(*spu);
                }
            }
        }

        spu_ids
    }

    /// Convert partition map into replica map
    pub fn partition_map_to_replica_map(&self) -> ReplicaMap {
        let mut replica_map: ReplicaMap = BTreeMap::new();

        for partition in &self.maps {
            replica_map.insert(partition.id as PartitionId, partition.replicas.clone());
        }

        replica_map
    }

    /// Validate partition map for assigned topics
    #[allow(clippy::explicit_counter_loop)]
    pub fn valid_partition_map(&self) -> Result<(), Error> {
        // there must be at least one partition in the partition map
        if self.maps.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "no assigned partitions found",
            ));
        }

        // assigned partitions must meet the following criteria
        //  ids:
        //      - must start with 0
        //      - must be in sequence, without gaps
        //  replicas:
        //      - must have at least one element
        //      - all replicas must have the same number of elements.
        //      - all elements must be unique
        //      - all elements must be positive integers
        let mut id = 0;
        let mut replica_len = 0;
        for partition in &self.maps {
            if id == 0 {
                // id must be 0
                if partition.id != id {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "assigned partitions must start with id 0",
                    ));
                }

                // replica must have elements
                replica_len = partition.replicas.len();
                if replica_len == 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "assigned replicas must have at least one spu id",
                    ));
                }
            } else {
                // id must be in sequence
                if partition.id != id {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "assigned partition ids must be in sequence and without gaps",
                    ));
                }

                // replica must have same number of elements as previous one
                if partition.replicas.len() != replica_len {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "all assigned replicas must have the same number of spu ids: {replica_len}"
                        ),
                    ));
                }
            }

            // all replica ids must be unique
            let mut sorted_replicas = partition.replicas.clone();
            sorted_replicas.sort_unstable();
            let unique_count = 1 + sorted_replicas
                .windows(2)
                .filter(|pair| pair[0] != pair[1])
                .count();
            if partition.replicas.len() != unique_count {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("duplicate spu ids found in assigned partition with id: {id}"),
                ));
            }

            // all ids must be positive numbers
            for spu_id in &partition.replicas {
                if *spu_id < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!("invalid spu id: {spu_id} in assigned partition with id: {id}"),
                    ));
                }
            }

            // increment id for next iteration
            id += 1;
        }

        Ok(())
    }
}

impl From<(PartitionCount, ReplicationFactor, IgnoreRackAssignment)> for TopicSpec {
    fn from(spec: (PartitionCount, ReplicationFactor, IgnoreRackAssignment)) -> Self {
        let (count, factor, rack) = spec;
        Self::new_computed(count, factor, Some(rack))
    }
}

/// convert from tuple with partition and replication with rack off
impl From<(PartitionCount, ReplicationFactor)> for TopicSpec {
    fn from(spec: (PartitionCount, ReplicationFactor)) -> Self {
        let (count, factor) = spec;
        Self::new_computed(count, factor, Some(false))
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionMap {
    pub id: PartitionId,
    pub replicas: Vec<SpuId>,
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CleanupPolicy {
    #[cfg_attr(feature = "use_serde", serde(rename = "segment"))]
    Segment(SegmentBasedPolicy),
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        CleanupPolicy::Segment(SegmentBasedPolicy::default())
    }
}

impl CleanupPolicy {
    pub fn retention_secs(&self) -> u32 {
        match self {
            CleanupPolicy::Segment(policy) => policy.retention_secs(),
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SegmentBasedPolicy {
    pub time_in_seconds: u32,
}

impl SegmentBasedPolicy {
    pub fn retention_secs(&self) -> u32 {
        self.time_in_seconds
    }
}

#[derive(Decoder, Encoder, Default, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct TopicStorageConfig {
    pub segment_size: Option<u32>,       // segment size
    pub max_partition_size: Option<u64>, // max partition size
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Snappy,
    Lz4,
    Any,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        CompressionAlgorithm::Any
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid compression type in topic")]
pub struct InvalidCompressionAlgorithm;

impl std::str::FromStr for CompressionAlgorithm {
    type Err = InvalidCompressionAlgorithm;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(CompressionAlgorithm::None),
            "gzip" => Ok(CompressionAlgorithm::Gzip),
            "snappy" => Ok(CompressionAlgorithm::Snappy),
            "lz4" => Ok(CompressionAlgorithm::Lz4),
            "any" => Ok(CompressionAlgorithm::Any),
            _ => Err(InvalidCompressionAlgorithm),
        }
    }
}
impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::None => write!(f, "none"),
            Self::Gzip => write!(f, "gzip"),
            Self::Snappy => write!(f, "snappy"),
            Self::Lz4 => write!(f, "lz4"),
            Self::Any => write!(f, "any"),
        }
    }
}

#[cfg(test)]
pub mod test {

    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_is_computed_topic() {
        let p1: PartitionMaps = vec![(1, vec![0]), (2, vec![2])].into();
        let t1 = ReplicaSpec::new_assigned(p1);
        assert!(!t1.is_computed());

        let t2 = ReplicaSpec::new_computed(0, 0, None);
        assert!(t2.is_computed());
    }

    #[test]
    fn test_valid_computed_replica_params() {
        // 0 is not a valid partition
        let t2_result = ReplicaSpec::valid_partition(&0);
        assert!(t2_result.is_err());
        assert_eq!(
            format!("{}", t2_result.unwrap_err()),
            "partition must be greater than 0"
        );

        let t3_result = ReplicaSpec::valid_partition(&1);
        assert!(t3_result.is_ok());

        // 0 is not a valid replication factor
        let t5_result = ReplicaSpec::valid_replication_factor(&0);
        assert!(t5_result.is_err());
        assert_eq!(
            format!("{}", t5_result.unwrap_err()),
            "replication factor must be greater than 0"
        );

        // positive numbers are OK
        let t6_result = ReplicaSpec::valid_replication_factor(&1);
        assert!(t6_result.is_ok());
    }

    //  Replica Map ids:
    //      - must start with 0
    //      - must be in sequence, without gaps
    #[test]
    fn test_replica_map_ids() {
        // id starts from 1 rather than 0
        let p1: PartitionMaps = vec![(1, vec![0]), (2, vec![2])].into();
        let p1_result = p1.valid_partition_map();
        assert!(p1_result.is_err());
        assert_eq!(
            format!("{}", p1_result.unwrap_err()),
            "assigned partitions must start with id 0"
        );

        // id has a gap
        let p2: PartitionMaps = vec![(0, vec![0]), (2, vec![2])].into();
        let p2_result = p2.valid_partition_map();
        assert!(p2_result.is_err());
        assert_eq!(
            format!("{}", p2_result.unwrap_err()),
            "assigned partition ids must be in sequence and without gaps"
        );

        // ids are out of sequence
        let p3: PartitionMaps = vec![(0, vec![0]), (2, vec![2]), (1, vec![1])].into();
        let p3_result = p3.valid_partition_map();
        assert!(p3_result.is_err());
        assert_eq!(
            format!("{}", p3_result.unwrap_err()),
            "assigned partition ids must be in sequence and without gaps"
        );

        // duplicate ids
        let p4: PartitionMaps = vec![(0, vec![0]), (1, vec![1]), (1, vec![1])].into();
        let p4_result = p4.valid_partition_map();
        assert!(p4_result.is_err());
        assert_eq!(
            format!("{}", p4_result.unwrap_err()),
            "assigned partition ids must be in sequence and without gaps"
        );

        // ids are ok
        let p5: PartitionMaps = vec![(0, vec![1]), (1, vec![1]), (2, vec![2])].into();
        let p5_result = p5.valid_partition_map();
        assert!(p5_result.is_ok());
    }

    //  Replica Map replicas:
    //      - replicas must have at least one element
    //      - all replicas must have the same number of elements
    //      - all elements must be unique
    //      - all elements must be positive integers
    #[test]
    fn test_replica_map_spu_ids() {
        // replicas must have at least one element
        let p1: PartitionMaps = vec![(0, vec![]), (1, vec![1])].into();
        let p1_result = p1.valid_partition_map();
        assert!(p1_result.is_err());
        assert_eq!(
            format!("{}", p1_result.unwrap_err()),
            "assigned replicas must have at least one spu id"
        );

        // all replicas must have the same number of elements
        let p2: PartitionMaps = vec![(0, vec![1, 2]), (1, vec![1])].into();
        let p2_result = p2.valid_partition_map();
        assert!(p2_result.is_err());
        assert_eq!(
            format!("{}", p2_result.unwrap_err()),
            "all assigned replicas must have the same number of spu ids: 2"
        );

        // all elements must be unique
        let p3: PartitionMaps = vec![(0, vec![1, 2]), (1, vec![1, 1])].into();
        let p3_result = p3.valid_partition_map();
        assert!(p3_result.is_err());
        assert_eq!(
            format!("{}", p3_result.unwrap_err()),
            "duplicate spu ids found in assigned partition with id: 1"
        );

        // all elements must be unique
        let p4: PartitionMaps = vec![(0, vec![3, 1, 2, 3])].into();
        let p4_result = p4.valid_partition_map();
        assert!(p4_result.is_err());
        assert_eq!(
            format!("{}", p4_result.unwrap_err()),
            "duplicate spu ids found in assigned partition with id: 0"
        );

        // all elements must be positive integers
        let p5: PartitionMaps = vec![(0, vec![1, 2]), (1, vec![1, -2])].into();
        let p5_result = p5.valid_partition_map();
        assert!(p5_result.is_err());
        assert_eq!(
            format!("{}", p5_result.unwrap_err()),
            "invalid spu id: -2 in assigned partition with id: 1"
        );
    }

    // Partitions repeatedly reference spu-ids. The purpose of
    // this API is to return a list of all unique SPUs
    #[test]
    fn test_unique_spus_in_partition_map() {
        // id starts from 1 rather than 0
        let p1: PartitionMaps =
            vec![(0, vec![0, 1, 3]), (1, vec![0, 2, 3]), (2, vec![1, 3, 4])].into();

        let p1_result = p1.unique_spus_in_partition_map();
        let expected_p1_result: Vec<SpuId> = vec![0, 1, 3, 2, 4];
        assert_eq!(p1_result, expected_p1_result);
    }

    // print in hex format:
    //   - println!("{:02x?}", dest);

    #[test]
    fn test_encode_decode_assigned_topic_spec() {
        let partition_map: PartitionMaps = vec![PartitionMap {
            id: 0,
            replicas: vec![5001, 5002],
        }]
        .into();
        let topic_spec = ReplicaSpec::Assigned(partition_map);
        let mut dest = vec![];

        // test encode
        let result = topic_spec.encode(&mut dest, 0);
        assert!(result.is_ok());
        let expected_dest = [
            0x00, // type
            0x00, 0x00, 0x00, 0x01, // partition cnt
            0x00, 0x00, 0x00, 0x00, // partition id
            0x00, 0x00, 0x00, 0x02, // replica cnt
            0x00, 0x00, 0x13, 0x89, // spu id: 5001
            0x00, 0x00, 0x13, 0x8a, // spu id: 5002
        ];
        assert_eq!(dest, expected_dest);

        // test encode
        let mut topic_spec_decoded = ReplicaSpec::default();
        let result = topic_spec_decoded.decode(&mut Cursor::new(&expected_dest), 0);
        assert!(result.is_ok());

        match topic_spec_decoded {
            ReplicaSpec::Assigned(partition_map) => {
                assert_eq!(
                    partition_map,
                    vec![PartitionMap {
                        id: 0,
                        replicas: vec![5001, 5002],
                    }]
                    .into()
                );
            }
            _ => panic!("expect assigned topic spec, found {topic_spec_decoded:?}"),
        }
    }

    #[test]
    fn test_encode_decode_computed_topic_spec() {
        let topic_spec = ReplicaSpec::Computed((2, 3, true).into());
        let mut dest = vec![];

        // test encode
        let result = topic_spec.encode(&mut dest, 0);
        assert!(result.is_ok());

        let expected_dest = [
            0x01, // type
            0x00, 0x00, 0x00, 0x02, // partition cnt
            0x00, 0x00, 0x00, 0x03, // replica cnt
            0x01, // ignore_rack_assignment
        ];
        assert_eq!(dest, expected_dest);

        // test encode
        let mut topic_spec_decoded = ReplicaSpec::default();
        let result = topic_spec_decoded.decode(&mut Cursor::new(&expected_dest), 0);
        assert!(result.is_ok());

        match topic_spec_decoded {
            ReplicaSpec::Computed(param) => {
                assert_eq!(param.partitions, 2);
                assert_eq!(param.replication_factor, 3);
                assert!(param.ignore_rack_assignment);
            }
            _ => panic!("expect computed topic spec, found {topic_spec_decoded:?}"),
        }
    }

    #[test]
    fn test_partition_map_str() {
        // Test multiple
        let p1: PartitionMaps =
            vec![(0, vec![0, 1, 3]), (1, vec![0, 2, 3]), (2, vec![1, 3, 4])].into();
        let spec = ReplicaSpec::new_assigned(p1);
        assert_eq!(
            spec.partition_map_str(),
            Some("0:[0, 1, 3], 1:[0, 2, 3], 2:[1, 3, 4]".to_string())
        );

        // Test empty
        let p2 = PartitionMaps::default();
        let spec2 = ReplicaSpec::new_assigned(p2);
        assert_eq!(spec2.partition_map_str(), Some("".to_string()));
    }
}
