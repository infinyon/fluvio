pub use flv_metadata::partition::*;

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
                _ => Err(Error::new(ErrorKind::Other,"not partition"))
            }

        }
    }


}
