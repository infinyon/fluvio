use std::path::PathBuf;

use anyhow::Result;
use tracing::debug;

use fluvio_future::task::run_block_on;
use fluvio_hub_util as hubutil;
use hubutil::HubAccess;

pub const CLI_CONFIG_PATH: &str = ".fluvio";
pub const CLI_CONFIG_HUB: &str = "hub"; // hub area of config

#[derive(Debug, clap::Parser)]
pub enum HubCmd {
    /// View or set the hub id
    ///
    /// A hub id is needed to publish packages
    /// and setting the id will also create a package
    /// signing key
    Id(IdOpt),
}

#[derive(Clone, Debug, clap::Args)]
pub struct IdOpt {
    hubid: Option<String>,

    #[clap(long, short)]
    verbose: bool,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}

impl HubCmd {
    pub(crate) fn process(&self) -> Result<()> {
        match &self {
            HubCmd::Id(opt) => opt.process(),
        }
    }
}

impl IdOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let mut access = HubAccess::default_load(&self.remote)?;
        debug!("hubid: {}", access.hubid);

        if let Some(hubid) = &self.hubid {
            // check if hubid is in profile, if so print it and end
            if !access.hubid.is_empty() {
                println!("hubid currently set to {}", access.hubid);
            }
            set_hubid(hubid, &mut access)?;
        } else if self.verbose {
            println!("hubid: {}", access.hubid);
            println!("pubkey:  {}", access.pubkey);
            println!("privkey: {}", access.pkgkey);
        } else {
            println!("{}", access.hubid);
        }
        Ok(())
    }
}

pub fn set_hubid(hubid: &str, access: &mut HubAccess) -> Result<()> {
    // if not: ask server if it exists
    if let Err(e) = run_block_on(access.create_hubid(hubid)) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    access.hubid = hubid.to_string();

    let cfgpath = def_hub_cfg_path()?;
    access.write_hash(cfgpath)?;
    print!("hubid set to {hubid}");
    Ok(())
}

/// default hub cfg data path
fn def_hub_cfg_path() -> Result<PathBuf> {
    let mut hub_cfg_path = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("missing config dir"))?;
    hub_cfg_path.push(CLI_CONFIG_PATH); // .fluvio
    hub_cfg_path.push(CLI_CONFIG_HUB);
    Ok(hub_cfg_path)
}
