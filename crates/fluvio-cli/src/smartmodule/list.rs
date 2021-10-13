use structopt::StructOpt;
use crate::Result;

/// List all existing SmartModules
#[derive(Debug, StructOpt)]
pub struct ListSmartModuleOpt {}

impl ListSmartModuleOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
