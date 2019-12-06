//!
//! # Fluvio -- Fetch Local SPU
//!
//! Communicates with SPU to fetch local parameters
//!
use log::trace;

use spu_api::spus::{FlvFetchLocalSpuRequest, FlvFetchLocalSpuResponse};
use spu_api::versions::ApiVersions;
use spu_api::SpuApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::spu_lookup_version;

// Query SPU Replica leader for offsets.
pub async fn spu_fetch_local_spu<'a>(
    conn: &'a mut Connection,
    versions: &'a ApiVersions,
) -> Result<FlvFetchLocalSpuResponse, CliError> {
    let request = FlvFetchLocalSpuRequest::default();
    let version = spu_lookup_version(SpuApiKey::FlvFetchLocalSpu, versions);

    trace!(
        "fetch local-spu req '{}': {:#?}",
        conn.server_addr(),
        request
    );

    let response = conn.send_request(request, version).await?;

    trace!("fetch local-spu '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
