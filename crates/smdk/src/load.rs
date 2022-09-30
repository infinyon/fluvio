use clap::Parser;
use anyhow::Result;

/// Load SmartModule into Fluvio cluster
#[derive(Debug, Parser)]
pub struct LoadOpt {
    name: String,
}
impl LoadOpt {
    pub(crate) fn process(&self) -> Result<()> {
        println!("loading SmartModule: {}", self.name);
        Ok(())
    }
}
