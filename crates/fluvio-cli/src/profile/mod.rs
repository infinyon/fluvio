//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!

mod sync;
mod current;
mod switch;
mod rename;
mod delete_profile;
mod delete_cluster;
mod list;
mod export;

use std::sync::Arc;

use clap::Parser;
use anyhow::Result;

use crate::common::output::Terminal;
use crate::profile::current::CurrentOpt;
use crate::profile::delete_cluster::DeleteClusterOpt;
use crate::profile::delete_profile::DeleteProfileOpt;
use crate::profile::switch::SwitchOpt;
use crate::profile::sync::SyncCmd;
use crate::profile::list::ListOpt;
use crate::profile::rename::RenameOpt;
use crate::profile::export::ExportOpt;

#[derive(Debug, Parser)]
pub struct ProfileOpt {
    #[clap(subcommand)]
    cmd: Option<ProfileCmd>,
}

impl ProfileOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        match self.cmd {
            Some(cmd) => cmd.process(out).await?,
            None => CurrentOpt {}.process()?,
        }

        Ok(())
    }
}

#[derive(Debug, Parser)]
#[clap(about = "Available Commands")]
pub enum ProfileCmd {
    /// Print the name of the current context
    #[clap(name = "current")]
    DisplayCurrent(CurrentOpt),

    /// Delete the named profile
    #[clap(name = "delete")]
    DeleteProfile(DeleteProfileOpt),

    /// Delete the named cluster
    #[clap(name = "delete-cluster")]
    DeleteCluster(DeleteClusterOpt),

    /// Display the entire Fluvio configuration
    #[clap(name = "list")]
    List(ListOpt),

    /// Rename a profile
    #[clap(name = "rename")]
    Rename(RenameOpt),

    /// Switch to the named profile
    #[clap(name = "switch")]
    Switch(SwitchOpt),

    /// Sync a profile from a cluster
    #[clap(subcommand, name = "sync")]
    Sync(SyncCmd),

    /// Export a profile for use in other applications
    #[clap(name = "export")]
    Export(ExportOpt),
}

impl ProfileCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        match self {
            Self::DisplayCurrent(current) => {
                current.process()?;
            }
            Self::DeleteProfile(delete_profile) => {
                delete_profile.process(out).await?;
            }
            Self::DeleteCluster(delete_cluster) => {
                delete_cluster.process().await?;
            }
            Self::List(list) => {
                list.process(out).await?;
            }
            Self::Rename(rename) => {
                rename.process()?;
            }
            Self::Switch(switch) => {
                switch.process(out).await?;
            }
            Self::Sync(sync) => {
                sync.process().await?;
            }
            Self::Export(export) => {
                export.process(out)?;
            }
        }

        Ok(())
    }
}
