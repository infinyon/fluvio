use kf_protocol_derive::Encode;
use kf_protocol_derive::Decode;

#[fluvio_kf(encode_discriminant)]
#[derive(PartialEq, Debug, Clone, Copy, Encode, Decode)]
#[repr(u16)]
pub enum AllKfApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroup = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersion = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
}

impl Default for AllKfApiKey {
    fn default() -> AllKfApiKey {
        AllKfApiKey::ApiVersion
    }
}

#[cfg(test)]
mod test {

    use crate::AllKfApiKey;
    use kf_protocol::Decoder;
    use kf_protocol::Encoder;
    use std::io::Cursor;

    #[test]
    fn test_decode_enum_not_enough() {
        let data = [0x11]; // only one value

        let mut value = AllKfApiKey::ApiVersion;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_api_enum() {
        let data = [0x00, 0x03];

        let mut value = AllKfApiKey::Metadata;
        let result = value.decode(&mut Cursor::new(&data), 0);
        assert!(result.is_ok());
        assert_eq!(value, AllKfApiKey::Metadata);
    }

    #[test]
    fn test_encode_enum() {
        let mut src = vec![];
        let value = AllKfApiKey::Metadata;
        let result = value.encode(&mut src, 0);
        assert!(result.is_ok());
        assert_eq!(src.len(), 2);
        assert_eq!(src[0], 0x00);
        assert_eq!(src[1], AllKfApiKey::Metadata as u8);
    }

}
