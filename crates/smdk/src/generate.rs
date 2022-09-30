use clap::Parser;
use anyhow::Result;

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateOpt {
    name: String,
}
impl GenerateOpt {
    pub(crate) fn process(&self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);
        Ok(())
    }
}
