use anyhow::Result;
use clap::Parser;

use fluvio_connector_package::metadata as mpkg;
use mpkg::ConnectorVisibility;

use crate::publish::find_connector_toml;
use crate::publish::CONNECTOR_TOML;

/// Set connector visibility to public
#[derive(Debug, Parser)]
pub struct SetPublicCmd {}

impl SetPublicCmd {
    pub(crate) fn process(&self) -> Result<()> {
        let smm_path = find_connector_toml()?;
        let mut cm = mpkg::ConnectorMetadata::from_toml_file(smm_path)?;
        if cm.package.visibility == ConnectorVisibility::Private {
            println!("warning: publishing a public package is irreversible");
        }
        cm.package.visibility = ConnectorVisibility::Public;
        cm.to_toml_file(CONNECTOR_TOML)
    }
}
