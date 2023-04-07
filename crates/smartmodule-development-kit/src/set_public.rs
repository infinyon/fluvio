// use std::path::{Path, PathBuf};

use anyhow::Result;
use cargo_builder::package::PackageInfo;
use clap::Parser;

use fluvio_controlplane_metadata::smartmodule as smpkg;
use smpkg::SmartModuleVisibility;

// use fluvio_future::task::run_block_on;
// use fluvio_hub_util as hubutil;
// use hubutil::{DEF_HUB_INIT_DIR, HubAccess, PackageMeta, PkgVisibility};

use crate::cmd::PackageCmd;
use crate::publish::find_smartmodule_toml;

/// Publish SmartModule to SmartModule Hub
#[derive(Debug, Parser)]
pub struct SetPublicOpt {
    #[clap(flatten)]
    package: PackageCmd,
}

impl SetPublicOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let opt = self.package.as_opt();
        let package_info = PackageInfo::from_options(&opt)?;

        let sm_path = find_smartmodule_toml(&package_info)?;
        let mut sm = smpkg::SmartModuleMetadata::from_toml(&sm_path)?;
        if sm.package.visibility == SmartModuleVisibility::Private {
            println!("warning: publishing a public package is irreversible");
        }
        sm.package.visibility = SmartModuleVisibility::Public;
        let tomlstr = toml::to_string(&sm)?;
        std::fs::write(sm_path, tomlstr)?;
        Ok(())
    }
}
