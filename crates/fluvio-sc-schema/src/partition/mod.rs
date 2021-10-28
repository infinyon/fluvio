pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use std::io::Error as IoError;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::{
        AdminSpec, NameFilter, ObjectDecoder,
        objects::{
            Metadata, ObjectApiWatchResponse, ObjectApiWatchRequest, WatchRequest, WatchResponse,
        },
    };
    use super::*;

    impl AdminSpec for PartitionSpec {
        type ListFilter = NameFilter;
        type WatchResponseType = Self;

        type DeleteKey = String;

        type ListType = Metadata<Self>;

        fn create_decoder() -> crate::CreateDecoder {
            panic!("Partition cannot be created directly")
        }
    }

    impl From<WatchRequest<PartitionSpec>> for (ObjectApiWatchRequest, ObjectDecoder) {
        fn from(req: WatchRequest<PartitionSpec>) -> Self {
            (
                ObjectApiWatchRequest::Partition(req),
                PartitionSpec::object_decoder(),
            )
        }
    }

    impl From<WatchResponse<PartitionSpec>> for (ObjectApiWatchResponse, ObjectDecoder) {
        fn from(response: WatchResponse<PartitionSpec>) -> Self {
            (
                ObjectApiWatchResponse::Partition(response),
                PartitionSpec::object_decoder(),
            )
        }
    }

    impl TryInto<WatchResponse<PartitionSpec>> for ObjectApiWatchResponse {
        type Error = IoError;

        fn try_into(self) -> Result<WatchResponse<PartitionSpec>, Self::Error> {
            match self {
                ObjectApiWatchResponse::Partition(response) => Ok(response),
                _ => Err(IoError::new(ErrorKind::Other, "not  partition")),
            }
        }
    }
}
