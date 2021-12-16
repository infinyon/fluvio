//!
//! # Delete Custom SPUs
//!
//! CLI tree to generate Delete Custom SPUs
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::customspu::CustomSpuSpec;
use fluvio::metadata::customspu::CustomSpuKey;
use crate::cli::ClusterCliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Clone)]
pub struct UnregisterCustomSpuOpt {
    /// SPU id
    #[structopt(short = "i", long = "id", required_unless = "name")]
    id: Option<i32>,

    /// SPU name
    #[structopt(
        short = "n",
        long = "name",
        value_name = "string",
        conflicts_with = "id"
    )]
    name: Option<String>,
}

impl UnregisterCustomSpuOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ClusterCliError> {
        let delete_key = self.validate()?;
        let admin = fluvio.admin().await;
        admin.delete::<CustomSpuSpec, _>(delete_key).await?;
        Ok(())
    }

    /// Validate cli options. Generate target-server and unregister custom spu config.
    fn validate(self) -> Result<CustomSpuKey, ClusterCliError> {
        let custom_spu = if let Some(name) = self.name {
            CustomSpuKey::Name(name)
        } else if let Some(id) = self.id {
            CustomSpuKey::Id(id)
        } else {
            return Err(IoError::new(ErrorKind::Other, "missing custom SPU name or id").into());
        };

        Ok(custom_spu)
    }
}
