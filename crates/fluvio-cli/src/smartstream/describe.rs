use structopt::StructOpt;
use crate::Result;

/// Print details about a given SmartStream
#[derive(Debug, StructOpt)]
pub struct DescribeSmartStreamOpt {
    name: String,
}

impl DescribeSmartStreamOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
