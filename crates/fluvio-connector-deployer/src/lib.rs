
use derive_builder::Builder;


pub enum Executable {
    Binary,
    Image(String),
}

pub struct Secret(String);


/// Describe deployment configuration
#[derive(Builder)]
pub struct Deployment {
     pub executable: dyn Executable,   // Executable to deploy, this can be binary, image, etc.
	 pub secrets: Vec<Secret>,          // List of Secrets
     pub config: ConnectorConfig,      // Configuration to pass along,	
     pub pkg:  ConnectorPkg             // Connector pkg definition
}


