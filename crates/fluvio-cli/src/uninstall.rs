use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::Parser;
use tracing::{debug, instrument};
use semver::Version;
use anyhow::Result;

use fluvio_channel::{LATEST_CHANNEL_NAME, FLUVIO_RELEASE_CHANNEL};
use fluvio_cli_common::{FLUVIO_ALWAYS_CHECK_UPDATES, error::PackageNotFound};
use fluvio_index::{PackageId, HttpAgent};
use fluvio_cli_common::install::{
    fetch_latest_version, fetch_package_file, install_bin, install_println, fluvio_extensions_dir,
};

use crate::metadata::subcommand_metadata;

#[derive(Parser, Debug)]
pub struct UninstallOpt;

impl UninstallOpt {
    pub async fn process(self) -> Result<()> {
        println!("Checking for updates...");

        Ok(())
    }
}
