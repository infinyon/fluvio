use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;

use cargo_builder::cargo::Cargo;

/// Calls cargo to clean the current working directory.
#[derive(Debug, Parser)]
pub struct CleanCmd {
    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,
}

impl CleanCmd {
    pub(crate) fn process(self) -> Result<()> {
        let cargo = Cargo::clean()
            .extra_arguments(self.extra_arguments)
            .build()?;

        cargo.run()
    }
}
