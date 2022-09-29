use clap::Parser;

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateOpt {
    name: String,
}
impl GenerateOpt {
    pub(crate) fn process(&self) {
        println!("Generating new SmartModule project: {}", self.name);
    }
}
