pub use fluvio_metadata::partition::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::*;
    use super::*;

    impl ListSpec for PartitionSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::Partition(filters)
        }
    }

    impl TryInto<Vec<Metadata<PartitionSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<PartitionSpec>>, Self::Error> {
            match self {
                ListResponse::Partition(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not partition")),
            }
        }
    }

    impl From<MetadataUpdate<PartitionSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<PartitionSpec>) -> Self {
            Self::Partition(update)
        }
    }

    impl TryInto<MetadataUpdate<PartitionSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<PartitionSpec>, Self::Error> {
            match self {
                WatchResponse::Partition(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not  partition")),
            }
        }
    }
}
