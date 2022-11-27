use clap::Parser;
use anyhow::Result;

/// Connector Development Kit
#[derive(Debug, Parser)]
pub enum CdkCommand {
    Test,
}

impl CdkCommand {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            Self::Test => {
                println!("Hello cdk");
            }
        }

        Ok(())
    }
}
