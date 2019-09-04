//!
//! # Fluvio SC - Create Processing
//!
//! Sends Create Custom SPU request to Fluvio Streaming Controller
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::spu::{FlvCreateCustomSpusRequest, FlvCreateCustomSpusResponse};
use sc_api::spu::{FlvCreateCustomSpuRequest, FlvEndPointMetadata};

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use crate::spu::custom::create::CreateCustomSpuConfig;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Create Custom Spu Request
pub fn process_sc_create_custom_spu(
    server_addr: SocketAddr,
    custom_spu_cfg: CreateCustomSpuConfig,
) -> Result<(), CliError> {
    let id = custom_spu_cfg.id;
    let name = custom_spu_cfg.name.clone();

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, custom_spu_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send create custom-spu '{}({})': {}", name, id, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let custom_spu_resp = &response.results[0];
                let response = handle_sc_response(
                    &name,
                    "custom-spu",
                    "created",
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
                        "cannot create custom spu '{}){})': communication error",
                        name, id
                    ),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send create request
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    custom_spu_cfg: CreateCustomSpuConfig,
) -> Result<FlvCreateCustomSpusResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&custom_spu_cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvCreateCustomSpus, &versions);

    trace!("create custom-spu req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("create custom-spu res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode CreateCustomSpuConfig in Fluvio format
fn encode_request(custom_spu_cfg: &CreateCustomSpuConfig) -> FlvCreateCustomSpusRequest {
    let create_custom_spu = FlvCreateCustomSpuRequest {
        id: custom_spu_cfg.id,
        name: custom_spu_cfg.name.clone(),
        public_server: FlvEndPointMetadata {
            host: custom_spu_cfg.public_server.host.clone(),
            port: custom_spu_cfg.public_server.port,
        },
        private_server: FlvEndPointMetadata {
            host: custom_spu_cfg.private_server.host.clone(),
            port: custom_spu_cfg.private_server.port,
        },
        rack: custom_spu_cfg.rack.clone(),
    };

    // generate request with 1 custom spu
    FlvCreateCustomSpusRequest {
        custom_spus: vec![create_custom_spu],
    }
}
