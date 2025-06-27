use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use clap::Parser;
use fluvio_future::task;
use fluvio_hub_util::cmd::{ConnectorHubListOpts, ConnectorHubDownloadOpts};
use fluvio_extension_common::PrintTerminal;

/// Work with Connectors hub
#[derive(Debug, Parser)]
pub enum HubCmd {
    #[command(name = "list")]
    List(ConnectorHubListOpts),
    #[command(name = "download")]
    Download(ConnectorHubDownloadOpts),

    /// Check a downloaded package
    #[command(name = "check", hide = true)]
    PkgCheck(HubPkgCheckOpts),
}

impl HubCmd {
    pub fn process(self) -> Result<()> {
        let terminal = Arc::new(PrintTerminal::new());
        match self {
            HubCmd::List(opt) => task::run_block_on(opt.process(terminal)),
            HubCmd::Download(opt) => task::run_block_on(opt.process(terminal)),
            HubCmd::PkgCheck(opt) => hub_pkg_check(opt),
        }
    }
}

#[derive(Debug, clap::Parser)]
pub struct HubPkgCheckOpts {
    pub ipkg_file: String,

    #[arg(short, long)]
    pub brief: bool,
}

fn hub_pkg_check(opts: HubPkgCheckOpts) -> Result<()> {
    use crate::utils::verify;
    use crate::publish::CONNECTOR_TOML;

    const TAG_ARCH: &str = "arch";

    if let Err(reason) = verify::connector_ipkg(&opts.ipkg_file) {
        println!("Error: {reason}");
    }

    if !opts.brief {
        // extract and print Connector.toml and manifest list
        let package_meta = fluvio_hub_util::package_get_meta(&opts.ipkg_file)?;
        let entries: Vec<&std::path::Path> = package_meta
            .manifest
            .iter()
            .map(std::path::Path::new)
            .collect();
        let connector_toml = entries
            .iter()
            .find(|e| {
                e.file_name()
                    .eq(&Some(std::ffi::OsStr::new(CONNECTOR_TOML)))
            })
            .ok_or_else(|| anyhow!("Package missing {CONNECTOR_TOML}"))?;
        let connector_toml_bytes =
            fluvio_hub_util::package_get_manifest_file(&opts.ipkg_file, connector_toml)?;
        let str_ctoml = std::str::from_utf8(&connector_toml_bytes)?;

        println!("=== Package: {}", &opts.ipkg_file);
        println!("\thub ref: {}", package_meta.pkg_name());
        println!("\ttags:");
        let tags = package_meta.tags.unwrap_or_default();
        for tag in &tags {
            println!("\t\t{}: {}", tag.tag, tag.value);
        }
        let has_arch = tags.iter().any(|tag| tag.tag == TAG_ARCH);
        if !has_arch {
            println!("\t\t{TAG_ARCH}: missing!");
        }
        println!();

        println!("=== Manifest entries:");
        for entry in entries {
            println!("\t{entry:?}");
        }
        println!();

        println!("=== {CONNECTOR_TOML}:\n{str_ctoml}");
        println!();
    }
    Ok(())
}
