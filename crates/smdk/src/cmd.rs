use clap::Parser;

use crate::generate::GenerateOpt;

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub enum SmdkCommand {
    /// Generate new SmartModule project
    Generate(GenerateOpt),
}

impl SmdkCommand {
    pub(crate) fn process(self) {
        match self {
            Self::Generate(opt) => {
                opt.process();
            }
        }
    }
}
