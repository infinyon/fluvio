use std::path::PathBuf;

use clap::Parser;
use anyhow::Result;

use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::HubAccess;

pub const CLI_CONFIG_PATH: &str = ".fluvio";
pub const CLI_CONFIG_HUB: &str = "hub"; // hub area of config

/// Set hubid in the SmartModule Hub
#[derive(Debug, Parser)]
pub struct SetHubidOpt {
    hubid: String,
}

impl SetHubidOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let cfgpath = def_hub_cfg_path()?;
        let mut access = HubAccess::load_path(&cfgpath, None)?;

        // check if hubid is in profile, if so print it and end
        if !access.hubid.is_empty() {
            print!("Hubid already set to {}", self.hubid);
            return Ok(());
        }

        // if not: ask server if it exists
        if let Err(e) = run_block_on(access.create_hubid(&self.hubid)) {
            eprintln!("{}", e);
            std::process::exit(1);
        }
        access.hubid = self.hubid.to_string();
        access.write_hash(cfgpath)?;
        Ok(())
    }
}

/// default hub cfg data path
fn def_hub_cfg_path() -> Result<PathBuf> {
    let mut hub_cfg_path = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("missing config dir"))?;
    hub_cfg_path.push(CLI_CONFIG_PATH); // .fluvio
    hub_cfg_path.push(CLI_CONFIG_HUB);
    Ok(hub_cfg_path)
}

pub fn get_hubaccess() -> Result<HubAccess> {
    let cfgpath = def_hub_cfg_path()?;
    let access = HubAccess::load_path(&cfgpath, None)?;
    Ok(access)
}
