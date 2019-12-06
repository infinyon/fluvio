use std::fmt::Display;

use future_aio::net::ToSocketAddrs;
use spu_api::versions::{ApiVersions, ApiVersionsRequest};
use spu_api::SpuApiKey;
use sc_api::apis::ScApiKey;
use kf_socket::KfSocket;

use crate::ClientError;

struct StreamController(KfSocket);



impl StreamController {

    /// Create controller build.  All configuration starts as default
    pub fn builder() -> StreamControllerBuilder {
        StreamControllerBuilder::new()
    }


    /// connect to Stream Controller
    pub async fn connect<A>(addr: A) -> Result<Self,ClientError> 
        where A: ToSocketAddrs + Display
    {

        let socket = KfSocket::connect(addr).await?;
        Ok(Self(socket))
    }

    /*
    async fn () -> Result<ApiVersions, CliError> {
        // Version is None, as we want API to request max_version.
        let response = conn
            .send_request(ApiVersionsRequest::default(), None)
            .await?;
        Ok(response.api_keys)
    }
    */

    /*
     /// perform initialization including api version
    pub async init() -> Result<(),ClientError> {

    }
    */
}


#[derive(Default)]
struct Config {
    addr: String
}


struct StreamControllerBuilder {
    config: Config
}

impl StreamControllerBuilder {

    pub fn new() -> Self {
        Self {
            config: Config {
                ..Default::default()
            }
        }
    }

   
    pub fn addr(mut self, addr: String) -> Self {
        self.config.addr = addr;
        self
    }

    pub async fn connect(&self) -> Result<StreamController,ClientError> {
        StreamController::connect(&self.config.addr).await
    }
}