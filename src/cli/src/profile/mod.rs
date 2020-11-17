//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!

use std::sync::Arc;
use structopt::StructOpt;

mod sync;
mod current;
mod switch;
mod delete_profile;
mod delete_cluster;
mod view;

use crate::{Result, Terminal};
use crate::profile::current::CurrentOpt;
use crate::profile::delete_cluster::DeleteClusterOpt;
use crate::profile::delete_profile::DeleteProfileOpt;
use crate::profile::switch::SwitchOpt;
use crate::profile::sync::SyncCmd;
use crate::profile::view::ViewOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ProfileCmd {
    /// Print the name of the current context
    #[structopt(name = "current")]
    DisplayCurrent(CurrentOpt),

    /// Delete the named profile
    #[structopt(name = "delete")]
    DeleteProfile(DeleteProfileOpt),

    /// Delete the named cluster
    #[structopt(name = "delete-cluster")]
    DeleteCluster(DeleteClusterOpt),

    /// Switch to the named profile
    #[structopt(name = "switch")]
    Switch(SwitchOpt),

    /// Sync a profile from a cluster
    #[structopt(name = "sync")]
    Sync(SyncCmd),

    /// Display entire configuration
    #[structopt(name = "view")]
    View(ViewOpt),
}

impl ProfileCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        match self {
            ProfileCmd::View(view) => {
                view.process().await?;
            }
            ProfileCmd::DisplayCurrent(current) => {
                current.process().await?;
            }
            ProfileCmd::Switch(switch) => {
                switch.process(out).await?;
            }
            ProfileCmd::DeleteProfile(delete_profile) => {
                delete_profile.process(out).await?;
            }
            ProfileCmd::DeleteCluster(delete_cluster) => {
                delete_cluster.process().await?;
            }
            ProfileCmd::Sync(sync) => {
                sync.process().await?;
            }
        }

        Ok(())
    }
}
