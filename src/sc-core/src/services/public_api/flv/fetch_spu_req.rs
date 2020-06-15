use log::{trace, debug};
use std::io::Error;

use kf_protocol::api::FlvErrorCode;
use kf_protocol::api::{RequestMessage, ResponseMessage};

use sc_api::spu::*;

use flv_metadata::spu::SpuType;
use flv_metadata::spu::SpuResolution;

use crate::core::ShareLocalStores;
use crate::core::spus::SpuKV;

pub async fn handle_fetch_spu_request(
    request: RequestMessage<FlvFetchSpusRequest>,
    metadata: ShareLocalStores,
) -> Result<ResponseMessage<FlvFetchSpusResponse>, Error> {
    // identify query type
    let (query_custom, query_type) = match request.request.req_spu_type {
        FlvRequestSpuType::Custom => (true, "custom"),
        FlvRequestSpuType::All => (false, "all"),
    };

    // traverse and convert spus to FLV response
    let mut flv_spu: Vec<FlvFetchSpuResponse> = Vec::default();
    for (name, spu) in metadata.spus().inner_store().read().iter() {
        // skip custom if necessary
        if query_custom && !spu.is_custom() {
            continue;
        }
        flv_spu.push(spu_store_metadata_to_spu_response(name, spu));
    }

    debug!(
        "flv fetch {} spus resp: {} items",
        query_type,
        flv_spu.len()
    );
    trace!("flv fetch {} spus resp {:#?}", query_type, flv_spu);

    // prepare response
    let mut response = FlvFetchSpusResponse::default();
    response.spus = flv_spu;

    Ok(request.new_response(response))
}

/// Encode Spus metadata into SPU FLV response
pub fn spu_store_metadata_to_spu_response(name: &str, spu: &SpuKV) -> FlvFetchSpuResponse {
    let public_ep = spu.public_endpoint();
    let private_ep = spu.private_endpoint();
    let flv_spu_type = match spu.spec().spu_type {
        SpuType::Custom => FlvSpuType::Custom,
        SpuType::Managed => FlvSpuType::Managed,
    };
    let flv_resolution = match spu.status().resolution {
        SpuResolution::Online => FlvSpuResolution::Online,
        SpuResolution::Offline => FlvSpuResolution::Offline,
        SpuResolution::Init => FlvSpuResolution::Init,
    };

    let flv_spu = FlvFetchSpu {
        id: *spu.id(),
        spu_type: flv_spu_type,
        public_ep: FlvEndPointMetadata {
            host: public_ep.host_string(),
            port: public_ep.port,
        },
        private_ep: FlvEndPointMetadata {
            host: private_ep.host.clone(),
            port: private_ep.port,
        },
        rack: spu.rack_clone(),
        resolution: flv_resolution,
    };

    FlvFetchSpuResponse {
        error_code: FlvErrorCode::None,
        name: name.to_owned(),
        spu: Some(flv_spu),
    }
}
