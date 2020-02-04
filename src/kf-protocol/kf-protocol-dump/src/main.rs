
mod public_api;

use std::path::PathBuf;
use std::io::Error as IoError;

use structopt::StructOpt;

use kf_protocol_api::KfRequestMessage;

pub(crate) use self::public_api::PublicRequest;

#[derive(Debug, StructOpt)]
pub struct DumpOpt {
    #[structopt(long = "resp", parse(from_os_str))]
    resp_file: Option<PathBuf>,
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,
}

macro_rules! decode {
    ($req:expr,$file:expr) => {
        println!(
            "response {:#?}",
            $req.decode_response_from_file($file, $req.header.api_version())?
        )
    };
}

pub(crate) fn dump_file(opt: DumpOpt) -> Result<(), IoError> {
    println!("opening file: {:#?}", opt.file_name);
    let api_request = PublicRequest::decode_from_file(opt.file_name)?;
    println!("request: {:#?}", api_request);
    if let Some(file) = opt.resp_file {
        match api_request {
            PublicRequest::KfApiVersionsRequest(req) => decode!(req, file),
            PublicRequest::KfProduceRequest(req) => decode!(req, file),
            PublicRequest::KfFetchRequest(req) => decode!(req, file),
            PublicRequest::KfJoinGroupRequest(req) => decode!(req, file),
            PublicRequest::KfUpdateMetadataRequest(req) => decode!(req, file),
        }
    }

    Ok(())
}

fn main() -> Result<(), IoError> {
    
    flv_util::init_logger();

    let opt = DumpOpt::from_args();
    dump_file(opt)
}

#[cfg(test)]
mod test {

    use log::debug;
    use std::io;
    use kf_protocol_api::AllKfApiKey;
    use kf_protocol_api::ResponseMessage;
    use kf_protocol_api::KfRequestMessage;
    use kf_protocol_message::fetch::DefaultKfFetchResponse;

    use crate::PublicRequest;

    #[test]
    fn test_fetch_request() -> Result<(), io::Error> {
        let file = "test-data/fetch-request1.bin";
        let msg = PublicRequest::decode_from_file(file)?;
        match msg {
            PublicRequest::KfFetchRequest(fetch_msg) => {
                assert_eq!(fetch_msg.header.api_key(), AllKfApiKey::Fetch as u16);
                assert_eq!(fetch_msg.request.min_bytes, 1);
                debug!("request: {:#?}", fetch_msg.request);
            }
            _ => assert!(false, "not fetch"),
        }
        Ok(())
    }

    #[test]
    fn test_fetch_response() -> Result<(), io::Error> {
        // [2018-12-20 21:47:16,551] lenght 13* 16 +1 -6 =

        let file = "test-data/fetch-response1.bin";
        let fetch_response = ResponseMessage::<DefaultKfFetchResponse>::decode_from_file(file, 7)?;
        let response = fetch_response.response;
        debug!("response: {:#?}", response);
        let wrapper = &(response.topics[0].partitions[0]);
        let batches = &(wrapper.records.batches);
        assert_eq!(batches.len(), 2);
        let values = batches[0].records[0]
            .value
            .inner_value_ref()
            .as_ref()
            .unwrap();
        let raw = String::from_utf8_lossy(values).to_string();
        assert_eq!(raw, "quick brown fox jump over the lazy dog");
        let values2 = batches[1].records[0]
            .value
            .inner_value_ref()
            .as_ref()
            .unwrap();
        let raw2 = String::from_utf8_lossy(values2).to_string();
        debug!("raw2: {}", raw2);
        Ok(())
    }
}
