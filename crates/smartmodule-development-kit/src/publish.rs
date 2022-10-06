use clap::Parser;
use anyhow::Result;

use crate::set_hubid::get_hubaccess;

use fluvio_hub_util as hubutil;
use hubutil::HubAccess;

use fluvio_future::task::run_block_on;

// use crate::wasm::WasmOption;

/// Publish SmartModule to SmartModule Hub
#[derive(Debug, Parser)]
pub struct PublishOpt {
    pub package_meta: Option<String>,

    /// do only the pack portion
    #[clap(long)]
    pack: bool,

    // given a packed file do only the push
    #[clap(long)]
    push: bool,
}
impl PublishOpt {
    pub(crate) fn process(&self) -> Result<()> {
        let access = get_hubaccess()?;

        match (self.pack, self.push) {
            (false, false) | (true, true) => {
                let pkgmetapath = self
                    .package_meta
                    .clone()
                    .unwrap_or_else(|| hubutil::DEF_HUB_PKG_META.to_string());
                let pkgdata = package_assemble(&pkgmetapath, &access)?;
                package_push(&pkgdata, &access)?;
            }

            // --pack only
            (true, false) => {
                let pkgmetapath = self
                    .package_meta
                    .clone()
                    .unwrap_or_else(|| hubutil::DEF_HUB_PKG_META.to_string());
                package_assemble(&pkgmetapath, &access)?;
            }

            // --push only, needs ipkg file
            (false, true) => {
                let pkgfile = &self
                    .package_meta
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("package file required for push"))?;
                package_push(pkgfile, &access)?;
            }
        }

        Ok(())
    }
}

pub fn package_assemble(pkgmeta: &str, access: &HubAccess) -> Result<String> {
    let pkgname = hubutil::package_assemble_and_sign(pkgmeta, access, None)?;
    println!("Package {} created", pkgname);
    Ok(pkgname)
}

pub fn package_push(pkgpath: &str, access: &HubAccess) -> Result<()> {
    if let Err(e) = run_block_on(hubutil::push_package(pkgpath, access)) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
    Ok(())
}
