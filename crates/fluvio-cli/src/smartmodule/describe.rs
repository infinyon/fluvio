use structopt::StructOpt;
use crate::Result;

/// Print details about a given SmartModule
#[derive(Debug, StructOpt)]
pub struct DescribeSmartModuleOpt {
    name: String,
}

impl DescribeSmartModuleOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
