//!
//! # Create Custom SPUs
//!
//! CLI tree to generate Create Custom SPUs
//!

use std::convert::TryFrom;

use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::customspu::CustomSpuSpec;
use flv_util::socket_helpers::ServerAddress;
use crate::cli::ClusterCliError;

#[derive(Debug, Parser)]
pub struct RegisterCustomSpuOpt {
    /// SPU id
    #[clap(short = 'i', long = "id")]
    id: i32,

    /// SPU name
    #[clap(short = 'n', long = "name", value_name = "string")]
    name: Option<String>,

    /// Rack name
    #[clap(short = 'r', long = "rack", value_name = "string")]
    rack: Option<String>,

    /// Public server::port
    #[clap(short = 'p', long = "public-server", value_name = "host:port")]
    public_server: String,

    /// Private server::port
    #[clap(short = 'v', long = "private-server", value_name = "host:port")]
    private_server: String,
}

impl RegisterCustomSpuOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let (name, spec) = self.validate()?;
        let admin = fluvio.admin().await;
        admin.create(name, false, spec).await?;
        Ok(())
    }

    /// Validate cli options. Generate target-server and register custom spu config.
    fn validate(self) -> Result<(String, CustomSpuSpec), ClusterCliError> {
        let cfg = (
            self.name.unwrap_or(format!("custom-spu-{}", self.id)),
            CustomSpuSpec {
                id: self.id,
                public_endpoint: ServerAddress::try_from(self.public_server)?.into(),
                private_endpoint: ServerAddress::try_from(self.private_server)?.into(),
                rack: self.rack,
            },
        );

        Ok(cfg)
    }
}
