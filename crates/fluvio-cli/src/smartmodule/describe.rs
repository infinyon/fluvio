use clap::Parser;
use crate::Result;

/// Print details about a given SmartModule
#[derive(Debug, Parser)]
pub struct DescribeSmartModuleOpt {
    _name: String,
}

impl DescribeSmartModuleOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
