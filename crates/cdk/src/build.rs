use std::collections::HashMap;
use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;

use cargo_builder::package::PackageInfo;

use crate::cmd::PackageCmd;
use crate::utils::build::{BuildOpts, build_connector};

const CLOUD_ARCH_TARGET: &str = "aarch64-unknown-linux-musl";

/// Build the Connector in the current working directory
#[derive(Debug, Parser)]
pub struct BuildCmd {
    #[clap(flatten)]
    package: PackageCmd,

    /// build optimized for running in the Infinyon Cloud environment
    #[arg(long, default_value_t = false)]
    cloud: bool,

    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,
}

impl BuildCmd {
    pub(crate) fn process(self) -> Result<()> {
        let mut opt = self.package.as_opt();
        if self.cloud {
            opt.target = CLOUD_ARCH_TARGET.to_owned();
        } else if target_not_specified() {
            let tmap = Self::target_map();
            if let Some(tgt) = tmap.get(&opt.target.as_str()) {
                opt.target = tgt.to_string();
            }
        }
        let package_info = PackageInfo::from_options(&opt)?;

        build_connector(
            &package_info,
            BuildOpts {
                cloud: self.cloud,
                release: opt.release,
                extra_arguments: self.extra_arguments,
            },
        )
    }

    /// Map to most supported native target
    fn target_map() -> HashMap<&'static str, &'static str> {
        let mut map = HashMap::new();
        map.insert("x86_64-unknown-linux-musl", "x86_64-unknown-linux-gnu");
        map
    }
}

fn target_not_specified() -> bool {
    let args = std::env::args().collect::<Vec<String>>();
    !args.iter().any(|arg| arg.contains("--target"))
}
