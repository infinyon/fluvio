//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!


use structopt::StructOpt;


#[derive(Debug,StructOpt)]
#[structopt(about = "Available Commands")]
pub enum Command {
    /// Display the current context
    #[structopt(name = "current-profile")]
    DisplayCurrentProfile,

    #[structopt(name = "use-profile")]
    UseProfile(UseProfile),

    /// set local context
    #[structopt(name="set-local")]
    SetLocal(SetLocal),

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
    #[structopt(value_name = "host:port")]
    pub local: String,
}

#[derive(Debug, StructOpt)]
pub struct UseProfile {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}
