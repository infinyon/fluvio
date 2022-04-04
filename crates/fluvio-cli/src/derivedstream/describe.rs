use clap::Parser;
use crate::Result;

/// Print details about a given DerivedStream
#[derive(Debug, Parser)]
pub struct DescribeDerivedStreamOpt {
    _name: String,
}

impl DescribeDerivedStreamOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
