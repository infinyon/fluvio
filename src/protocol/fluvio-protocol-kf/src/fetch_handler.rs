use std::fmt::Debug;

use kf_protocol::Decoder;
use kf_protocol::Encoder;

use kf_protocol_api::RecordSet;

use crate::fetch::{KfFetchResponse, KfFetchRequest};
use crate::fetch::FetchableTopicResponse;

pub type DefaultKfFetchRequest = KfFetchRequest<RecordSet>;
pub type DefaultKfFetchResponse = KfFetchResponse<RecordSet>;

// -----------------------------------
// Implementation
// -----------------------------------

impl<R> KfFetchResponse<R>
where
    R: Encoder + Decoder + Debug,
{
    pub fn find_topic(&self, topic: &String) -> Option<&FetchableTopicResponse<R>>
    where
        R: Debug,
    {
        for r_topic in &self.topics {
            if r_topic.name == *topic {
                return Some(r_topic);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {

    use super::DefaultKfFetchRequest;

    #[test]
    fn test_request() {
        let _ = DefaultKfFetchRequest::default();
        assert!(true);
    }
}
