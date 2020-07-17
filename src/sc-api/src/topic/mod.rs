

pub use flv_metadata::topic::*;

mod convert {

    use std::convert::TryInto;
    use std::io::Error;
    use std::io::ErrorKind;

    use crate::objects::*;
    use super::*;

    
    impl From<TopicSpec> for AllCreatableSpec {
        fn from(spec: TopicSpec) -> Self {
            Self::Topic(spec)
        }
    }


    impl DeleteSpec for TopicSpec  {

        fn into_request<K>(key: K) -> DeleteRequest where K: Into<Self::DeleteKey> {
            DeleteRequest::Topic(key.into())
        }

    }

    impl ListSpec for TopicSpec {

        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::Topic(filters)
        }
        

    }

    impl TryInto<Vec<Metadata<TopicSpec>>> for ListResponse {
        type Error = Error;
        
        fn try_into(self) -> Result<Vec<Metadata<TopicSpec>>, Self::Error> {

            match self {
                ListResponse::Topic(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other,"not spg"))
            }

        }
    }

}