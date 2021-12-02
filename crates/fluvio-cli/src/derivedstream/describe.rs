use structopt::StructOpt;
use crate::Result;

/// Print details about a given DerivedStream
#[derive(Debug, StructOpt)]
pub struct DescribeDerivedStreamOpt {
    _name: String,
}

impl DescribeDerivedStreamOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
