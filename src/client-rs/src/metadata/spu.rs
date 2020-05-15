//!
//! # Fluvio SC - List SPU Metadata
//!
//!  Serializable metadata for List SPU result
//!
use serde::Serialize;

use sc_api::spu::FlvFetchSpuResponse;
use sc_api::spu::FlvFetchSpu;
use sc_api::spu::FlvSpuResolution;
use sc_api::spu::FlvSpuType;
use sc_api::errors::FlvErrorCode;

use super::endpoint::Endpoint;


#[derive(Serialize, Debug)]
pub struct SpuMetadata {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<FlvErrorCode>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spu: Option<Spu>,
}


impl From<FlvFetchSpuResponse> for SpuMetadata {
    fn from(spu: FlvFetchSpuResponse) -> Self {
        Self::new(spu)
    }
}


impl  SpuMetadata {
    pub fn new(fetch_spu_resp: FlvFetchSpuResponse) -> Self {

        let (f_spu,f_error_code,f_name) = (fetch_spu_resp.spu,fetch_spu_resp.error_code,fetch_spu_resp.name);
        // if spu is present, convert it
        let spu = if let Some(fetched_spu) = f_spu {
            Some(Spu::new(f_name.clone(), fetched_spu))
        } else {
            None
        };

        // if error is present, convert it
        let error = if f_error_code.is_error() {
            Some(fetch_spu_resp.error_code)
        } else {
            None
        };

        // spu metadata with all parameters converted
        Self {
            name: f_name,
            error: error,
            spu: spu,
        }
    }
}


#[derive(Serialize, Debug)]
pub struct Spu {
    pub id: i32,
    pub name: String,
    pub spu_type: SpuType,
    pub public_server: Endpoint,
    pub private_server: Endpoint,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,

    pub status: SpuResolution,
}

#[derive(Serialize, Debug)]
pub enum SpuType {
    Custom,
    Managed,
}

#[derive(Serialize, Debug)]
pub enum SpuResolution {
    Online,
    Offline,
    Init,
}


impl Spu {
    pub fn new(spu_name: String, fetched_spu: FlvFetchSpu) -> Self {
        let (public_eps,private_ep) = (fetched_spu.public_ep,fetched_spu.private_ep);

        Spu {
            id: fetched_spu.id,
            name: spu_name.clone(),
            spu_type: SpuType::new(&fetched_spu.spu_type),

            public_server: Endpoint::new(public_eps.host,public_eps.port),
            private_server: Endpoint::new(private_ep.host, private_ep.port),

            rack: fetched_spu.rack.clone(),
            status: SpuResolution::new(&fetched_spu.resolution),
        }
    }

    pub fn type_label(&self) -> &'static str {
        SpuType::type_label(&self.spu_type)
    }

    pub fn status_label(&self) -> &'static str {
        SpuResolution::resolution_label(&self.status)
    }
}

impl SpuType {
    pub fn new(flv_spu_type: &FlvSpuType) -> Self {
        match flv_spu_type {
            FlvSpuType::Custom => SpuType::Custom,
            FlvSpuType::Managed => SpuType::Managed,
        }
    }

    pub fn type_label(spu_type: &SpuType) -> &'static str {
        match spu_type {
            SpuType::Custom => "custom",
            SpuType::Managed => "managed",
        }
    }
}

impl SpuResolution {
    pub fn new(flv_spu_resolution: &FlvSpuResolution) -> Self {
        match flv_spu_resolution {
            FlvSpuResolution::Online => SpuResolution::Online,
            FlvSpuResolution::Offline => SpuResolution::Offline,
            FlvSpuResolution::Init => SpuResolution::Init,
        }
    }

    pub fn resolution_label(resolution: &SpuResolution) -> &'static str {
        match resolution {
            SpuResolution::Online => "online",
            SpuResolution::Offline => "offline",
            SpuResolution::Init => "initializing",
        }
    }
}
