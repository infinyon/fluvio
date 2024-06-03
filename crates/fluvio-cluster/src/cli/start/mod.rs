use std::ops::Deref;
use std::{fmt, str::FromStr};
use std::path::PathBuf;

use clap::{Parser, Args};
use semver::Version;
use anyhow::Result;

mod local;
mod k8;
mod sys;

use crate::InstallationType;

use crate::cli::options::{SpuCliConfig, ClusterConnectionOpts, K8Install};

pub fn default_log_directory() -> PathBuf {
    let base = fluvio_cli_common::install::fluvio_base_dir().unwrap_or(std::env::temp_dir());
    base.join("log")
}

#[derive(Debug, Clone)]
pub struct DefaultLogDirectory(PathBuf);

impl Default for DefaultLogDirectory {
    fn default() -> Self {
        Self(default_log_directory())
    }
}

impl fmt::Display for DefaultLogDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl FromStr for DefaultLogDirectory {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.into()))
    }
}

impl Deref for DefaultLogDirectory {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Parser)]
pub struct StartOpt {
    /// use local image
    #[arg(long)]
    pub develop: bool,

    #[clap(flatten)]
    pub k8_config: K8Install,

    #[clap(flatten)]
    pub spu_config: SpuCliConfig,

    #[arg(long)]
    pub skip_profile_creation: bool,

    #[clap(flatten)]
    pub connection_config: ClusterConnectionOpts,

    /// data dir
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// number of SPU
    #[arg(long, default_value = "1")]
    pub spu: u16,

    /// RUST_LOG options
    #[arg(long)]
    pub rust_log: Option<String>,

    /// log dir
    #[arg(long, default_value_t)]
    pub log_dir: DefaultLogDirectory,

    #[arg(long)]
    /// installing/upgrade sys only
    sys_only: bool,

    /// Whether to skip pre-install checks, defaults to false
    #[arg(long)]
    pub skip_checks: bool,
    
    /// Tries to setup necessary environment for cluster startup
    #[arg(long)]
    pub setup: bool,
    
    /// Service Type
    #[arg(long)]
    pub service_type: Option<String>,

    #[command(flatten)]
    pub installation_type: IntallationTypeOpt,
}

#[derive(Debug, Args)]
#[group(multiple = false)]
pub struct IntallationTypeOpt {
    /// install local spu/sc
    #[arg(long)]
    local: bool,

    /// install local spu/sc with metadata stored in K8s
    #[arg(long)]
    local_k8: bool,

    /// install on K8s
    #[arg(long)]
    k8: bool,

    /// Start SC in read only mode
    #[arg(long, value_name = "config path")]
    read_only: Option<PathBuf>,
}

impl StartOpt {
    pub async fn process(self, platform_version: Version) -> Result<()> {
        use crate::cli::start::local::process_local;
        use crate::cli::start::sys::process_sys;
        use crate::cli::start::k8::process_k8;

        if self.sys_only {
            process_sys(&self, false /* TODO: I can't find a code-path where process_sys is called with upgrade=True */)?;
        } else if self.installation_type.is_local_group() {
            process_local(self, platform_version).await?;
        } else {
            process_k8(self, platform_version).await?;
        }

        Ok(())
    }
}

impl IntallationTypeOpt {
    fn is_local_group(&self) -> bool {
        !matches!(self.get_or_default(), InstallationType::K8)
    }

    pub fn get(&self) -> Option<InstallationType> {
        match (self.local, self.local_k8, &self.read_only, &self.k8) {
            (true, _, _, _) => Some(InstallationType::Local),
            (_, true, _, _) => Some(InstallationType::LocalK8),
            (_, _, Some(_), _) => Some(InstallationType::ReadOnly),
            (_, _, _, true) => Some(InstallationType::K8),
            _ => None,
        }
    }

    pub fn set(&mut self, installation_type: InstallationType) {
        let (local, local_k8, k8, read_only) = match installation_type {
            InstallationType::K8 => (false, false, true, None),
            InstallationType::Local => (true, false, false, None),
            InstallationType::LocalK8 => (false, true, false, None),
            InstallationType::ReadOnly => (false, false, false, Some(Default::default())),
            InstallationType::Docker => (false, false, false, None),
            InstallationType::Cloud => (false, false, false, None),
        };
        self.local = local;
        self.local_k8 = local_k8;
        self.k8 = k8;
        self.read_only = read_only;
    }

    pub fn get_or_default(&self) -> InstallationType {
        self.get().unwrap_or(InstallationType::Local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_installation_type_set() {
        //given
        let mut opt = IntallationTypeOpt {
            local: Default::default(),
            local_k8: Default::default(),
            k8: Default::default(),
            read_only: Default::default(),
        };

        //when
        assert_eq!(opt.get(), None);

        opt.set(InstallationType::K8);
        assert_eq!(opt.get(), Some(InstallationType::K8));

        opt.set(InstallationType::Local);
        assert_eq!(opt.get(), Some(InstallationType::Local));

        opt.set(InstallationType::LocalK8);
        assert_eq!(opt.get(), Some(InstallationType::LocalK8));

        opt.set(InstallationType::ReadOnly);
        assert_eq!(opt.get(), Some(InstallationType::ReadOnly));
    }
}
