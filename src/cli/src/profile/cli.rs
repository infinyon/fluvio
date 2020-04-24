//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!

use crate::tls::TlsConfig;
use structopt::StructOpt;


#[derive(Debug,StructOpt)]
#[structopt(about = "Available Commands")]
pub enum Command {
    /// Display the current context
    #[structopt(name = "current-profile")]
    DisplayCurrentProfile,

    #[structopt(name = "use-profile")]
    UseProfile(UseProfile),

    /// set profile to local servers 
    #[structopt(name="set-local-profile")]
    SetLocalProfile(SetLocal),

    /// set profile to kubernetes cluster
    #[structopt(name="set-k8-profile")]
    SetK8Profile(SetK8),

     /// Display entire configuration
    #[structopt(name = "view")]
    View,
}

#[derive(Debug, StructOpt)]
pub struct ProfileCommand {

    /// set local context with new sc address
  //  #[structopt(short,long, value_name = "host:port")]
  //  pub local: Option<String>,

    #[structopt(subcommand)]
    pub cmd: Command
}



#[derive(Debug, StructOpt)]
pub struct SetLocal {
    #[structopt(value_name = "host:port",default_value="localhost:9003")]
    pub local: String,

    #[structopt(flatten)]
    pub tls: TlsConfig

}

#[derive(Debug, StructOpt)]
pub struct SetK8 {
    /// kubernetes namespace, 
    #[structopt(long,short,value_name = "namespace")]
    pub namespace: Option<String>,

    /// profile name
    #[structopt(value_name = "name")]
    pub name: Option<String>,

    #[structopt(flatten)]
    pub tls: TlsConfig
}


#[derive(Debug, StructOpt)]
pub struct UseProfile {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}
