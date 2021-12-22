use structopt::StructOpt;
use crate::Result;

/// Print details about a given SmartModule
#[derive(Debug, StructOpt, Clone)]
pub struct DescribeSmartModuleOpt {
    _name: String,
}

impl DescribeSmartModuleOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
