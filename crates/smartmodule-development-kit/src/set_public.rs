// use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

use fluvio_controlplane_metadata::smartmodule as smpkg;
use smpkg::SmartModuleVisibility;

// use fluvio_future::task::run_block_on;
// use fluvio_hub_util as hubutil;
// use hubutil::{DEF_HUB_INIT_DIR, HubAccess, PackageMeta, PkgVisibility};

use crate::publish::find_smartmodule_toml;
use crate::publish::SMARTMODULE_TOML;

/// Publish SmartModule to SmartModule Hub
#[derive(Debug, Parser)]
pub struct SetPublicOpt {}

impl SetPublicOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let smm_path = find_smartmodule_toml()?;
        let mut sm = smpkg::SmartModuleMetadata::from_toml(smm_path)?;
        if sm.package.visibility == SmartModuleVisibility::Private {
            println!("warning: publishing a public package is irreversible");
        }
        sm.package.visibility = SmartModuleVisibility::Public;
        let tomlstr = toml::to_string(&sm)?;
        std::fs::write(SMARTMODULE_TOML, &tomlstr)?;
        Ok(())
    }
}
