use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;

use cargo_builder::package::PackageInfo;

use crate::cmd::PackageCmd;
use crate::utils::build::{BuildOpts, build_connector};

/// Build the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct BuildCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,
}

impl BuildCmd {
    pub(crate) fn process(self) -> Result<()> {
        let opt = self.package.as_opt();
        let p = PackageInfo::from_options(&opt)?;

        build_connector(
            &p,
            BuildOpts {
                release: opt.release,
                extra_arguments: self.extra_arguments,
            },
        )
    }
}
