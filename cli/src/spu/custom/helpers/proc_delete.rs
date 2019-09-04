//!
//! # Fluvio SC - Delete Processing
//!
//! Sends Delete Custom SPU request to Fluvio Streaming Controller
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::spu::{FlvDeleteCustomSpusRequest, FlvDeleteCustomSpusResponse};
use sc_api::spu::FlvCustomSpu;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use crate::spu::custom::delete::CustomSpu;
use crate::spu::custom::delete::DeleteCustomSpuConfig;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Delete Custom Spu Request
pub fn process_sc_delete_custom_spu(
    server_addr: SocketAddr,
    custom_spu_cfg: DeleteCustomSpuConfig,
) -> Result<(), CliError> {
    let custom_spu_str = format!("{}", custom_spu_cfg.custom_spu);

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, custom_spu_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send delete custom-spu '{}': {}", custom_spu_str, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let custom_spu_resp = &response.results[0];
                let response = handle_sc_response(
                    &custom_spu_str,
                    "custom-spu",
                    "deleted",
                    "",
                    &custom_spu_resp.error_code,
                    &custom_spu_resp.error_message,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!(
                        "cannot delete custom spu '{}': communication error",
                        custom_spu_str
                    ),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send delete request
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    custom_spu_cfg: DeleteCustomSpuConfig,
) -> Result<FlvDeleteCustomSpusResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&custom_spu_cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvDeleteCustomSpus, &versions);

    trace!("delete custom-spu req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("delete custom-spu res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode DeleteCustomSpuConfig in Fluvio format
fn encode_request(custom_spu_cfg: &DeleteCustomSpuConfig) -> FlvDeleteCustomSpusRequest {
    let flv_custom_spu = match &custom_spu_cfg.custom_spu {
        CustomSpu::Name(spu_name) => FlvCustomSpu::Name(spu_name.clone()),
        CustomSpu::Id(id) => FlvCustomSpu::Id(*id),
    };

    // generate request with 1 custom spu
    FlvDeleteCustomSpusRequest {
        custom_spus: vec![flv_custom_spu],
    }
}
