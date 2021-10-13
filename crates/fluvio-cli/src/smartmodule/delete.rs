use structopt::StructOpt;
use crate::Result;

/// Delete an existing SmartModule with the given name
#[derive(Debug, StructOpt)]
pub struct DeleteSmartModuleOpt {
    name: String,
}

impl DeleteSmartModuleOpt {
    pub fn process(self) -> Result<()> {
        todo!()
    }
}
