use structopt::StructOpt;
use crate::Result;

/// Print details about a given SmartModule
#[derive(Debug, StructOpt)]
pub struct DescribeSmartStreamOpt {
    name: String,
}

impl DescribeSmartStreamOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
