use std::fmt::Debug;

use anyhow::{Result};
use clap::Parser;

use cargo_builder::package::{PackageInfo};

use crate::build::PackageCmd;

/// Builds the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct Deploy {
    #[clap(flatten)]
    package: PackageCmd,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

impl Deploy {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let _p = PackageInfo::from_options(&opt).map_err(|e| anyhow::anyhow!(e))?;

        // TODO
        // find binary
        // spawn connector process and pass config file

        Ok(())
    }
}
