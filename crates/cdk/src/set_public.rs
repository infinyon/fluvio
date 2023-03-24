use anyhow::Result;
use cargo_builder::package::PackageInfo;
use clap::Parser;

use fluvio_connector_package::metadata as mpkg;
use mpkg::ConnectorVisibility;

use crate::cmd::PackageCmd;
use crate::publish::find_connector_toml;
use crate::publish::CONNECTOR_TOML;

/// Set connector visibility to public
#[derive(Debug, Parser)]
pub struct SetPublicCmd {
    #[clap(flatten)]
    package: PackageCmd,
}

impl SetPublicCmd {
    pub(crate) fn process(&self) -> Result<()> {
        let opt = self.package.as_opt();
        let package_info = PackageInfo::from_options(&opt)?;

        let smm_path = find_connector_toml(&package_info)?;
        let mut cm = mpkg::ConnectorMetadata::from_toml_file(smm_path)?;
        if cm.package.visibility == ConnectorVisibility::Private {
            println!("warning: publishing a public package is irreversible");
        }
        cm.package.visibility = ConnectorVisibility::Public;
        cm.to_toml_file(CONNECTOR_TOML)
    }
}
