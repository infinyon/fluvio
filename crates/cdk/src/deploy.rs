use std::{
    fmt::Debug,
    path::{PathBuf, Path},
    ffi::OsStr,
    fs::{File, Permissions},
    io::Write,
};

use anyhow::{Result, Context, anyhow};
use clap::{Parser, Subcommand};
use tempfile::TempDir;
use tracing::{debug, trace};

use cargo_builder::package::PackageInfo;
use fluvio_connector_deployer::{Deployment, DeploymentType, LogLevel};
use fluvio_connector_package::metadata::ConnectorMetadata;
use fluvio_connector_package::config::ConnectorConfig;

use crate::cmd::PackageCmd;
use crate::utils::build::{BuildOpts, build_connector};

const CONNECTOR_METADATA_FILE_NAME: &str = "Connector.toml";

/// Deploy the Connector from the current working directory
#[derive(Debug, Parser)]
pub struct DeployCmd {
    #[clap(flatten)]
    package: PackageCmd,

    #[command(subcommand)]
    operation: DeployOperationCmd,

    /// Extra arguments to be passed to cargo
    #[arg(raw = true)]
    extra_arguments: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum DeployOperationCmd {
    #[command(flatten)]
    Start(DeployStartCmd),
    #[command(flatten)]
    Shutdown(DeployShutdownCmd),
    #[command(flatten)]
    List(DeployListCmd),
    #[command(flatten)]
    Log(DeployLogCmd),
}

#[derive(Debug, Subcommand)]
enum DeployStartCmd {
    /// Start new deployment for the given connector config
    // As long as there is only one deployment type, we omit to specify its name
    #[command(name = "start")]
    Local {
        /// Path to configuration file in YAML format
        #[arg(short, long, value_name = "PATH")]
        config: PathBuf,

        /// Path to file with secrets. Secrets are 'key=value' pairs separated by the new line character. Optional
        #[arg(short, long, value_name = "PATH")]
        secrets: Option<PathBuf>,

        /// Deploy from local package file
        #[arg(long = "ipkg", value_name = "PATH")]
        ipkg_file: Option<PathBuf>,

        /// Log level for the connector process
        #[arg(long, value_name = "LOG_LEVEL", default_value_t)]
        log_level: LogLevel,
    },
}

#[derive(Debug, Subcommand)]
enum DeployShutdownCmd {
    /// Shutdown the Connector's deployment
    // As long as there is only one deployment type, we omit to specify its name
    #[command(name = "shutdown")]
    #[clap(group(
        clap::ArgGroup::new("name-source")
        .required(true)
        .args(&["config", "name"]),
    ))]
    Local {
        /// Path to configuration file in YAML format
        #[arg(short, long, conflicts_with = &"name", value_name = "PATH")]
        config: Option<PathBuf>,

        /// Name of the connector to shutdown
        #[arg(short, long, conflicts_with = &"config", value_name = "CONNECTOR_NAME")]
        name: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum DeployListCmd {
    /// Print the list of all deployed connectors
    // As long as there is only one deployment type, we omit to specify its name
    #[command(name = "list")]
    Local,
}

#[derive(Debug, Subcommand)]
enum DeployLogCmd {
    /// Print the connector's logs
    // As long as there is only one deployment type, we omit to specify its name
    #[command(name = "log")]
    #[clap(group(
        clap::ArgGroup::new("name-source")
        .required(true)
        .args(&["config", "name"]),
    ))]
    Local {
        /// Path to configuration file in YAML format
        #[arg(short, long, conflicts_with = &"name", value_name = "PATH")]
        config: Option<PathBuf>,

        /// Name of the running connector
        #[arg(short, long, conflicts_with = &"config", value_name = "CONNECTOR_NAME")]
        name: Option<String>,
    },
}

impl DeployCmd {
    pub(crate) fn process(self) -> Result<()> {
        let DeployCmd {
            package,
            operation,
            extra_arguments,
        } = self;
        operation.process(package, extra_arguments)
    }
}

impl DeployOperationCmd {
    pub(crate) fn process(self, package: PackageCmd, _extra_arguments: Vec<String>) -> Result<()> {
        match self {
            Self::Start(deployment_type) => deployment_type.process(package),
            Self::Shutdown(deployment_type) => deployment_type.process(package),
            Self::List(deployment_type) => deployment_type.process(),
            Self::Log(deployment_type) => deployment_type.process(package),
        }
    }
}

impl DeployStartCmd {
    pub(crate) fn process(self, package: PackageCmd) -> Result<()> {
        match self {
            Self::Local {
                config,
                secrets,
                ipkg_file,
                log_level,
            } => deploy_local(package, config, secrets, ipkg_file, log_level),
        }
    }
}

impl DeployListCmd {
    pub(crate) fn process(self) -> Result<()> {
        match self {
            Self::Local => local_index::print(),
        }
    }
}

impl DeployShutdownCmd {
    pub(crate) fn process(self, package: PackageCmd) -> Result<()> {
        match self {
            Self::Local { config, name } => shutdown_local(package, config, name),
        }
    }
}

impl DeployLogCmd {
    pub(crate) fn process(self, package: PackageCmd) -> Result<()> {
        match self {
            Self::Local { config, name } => print_local_log(package, config, name),
        }
    }
}

fn deploy_local(
    package_cmd: PackageCmd,
    config: PathBuf,
    secrets: Option<PathBuf>,
    ipkg_file: Option<PathBuf>,
    log_level: LogLevel,
) -> Result<()> {
    let opt = package_cmd.as_opt();
    let mut tmp_dir: Option<PathBuf> = None;

    let (executable, connector_metadata) = match ipkg_file {
        Some(ipkg_file) => {
            let (exec, cmeta) =
                from_ipkg_file(ipkg_file).context("Failed to deploy from ipkg file")?;
            let mut exec_dir = exec.clone();
            exec_dir.pop();
            tmp_dir = Some(exec_dir);
            (exec, cmeta)
        }
        None => {
            let package_info = PackageInfo::from_options(&opt)?;
            build_connector(&package_info, BuildOpts::with_release(opt.release.as_str()))?;
            from_cargo_package(&package_info)
                .context("Failed to deploy from within cargo package directory")?
        }
    };

    let metaconfig = ConnectorConfig::from_file(&config)
        .map_err(|e| anyhow!("Couldn't read config file {}: {e}", config.display()))?;
    let mut log_path = std::env::current_dir()?;
    log_path.push(metaconfig.name());
    log_path.set_extension("log");

    let mut builder = Deployment::builder();
    builder
        .executable(executable)
        .config(config)
        .secrets(secrets)
        .pkg(connector_metadata)
        .log_level(log_level)
        .deployment_type(DeploymentType::Local {
            output_file: Some(log_path),
            tmp_dir,
        });
    let result = builder.deploy()?;
    local_index::store(result)
}

fn shutdown_local(
    package_cmd: PackageCmd,
    config: Option<PathBuf>,
    name: Option<String>,
) -> Result<()> {
    let name = match (config, name) {
        (Some(config_path), None) => connector_name_from_config(package_cmd, config_path)?,
        (None, Some(name)) => name,
        _ => return Err(anyhow!("Either name or config must be specified")),
    };

    local_index::delete_by_name(&name)
}

fn print_local_log(
    package_cmd: PackageCmd,
    config: Option<PathBuf>,
    name: Option<String>,
) -> Result<()> {
    let name = match (config, name) {
        (Some(config_path), None) => connector_name_from_config(package_cmd, config_path)?,
        (None, Some(name)) => name,
        _ => return Err(anyhow!("Either name or config must be specified")),
    };

    local_index::print_log(&name)
}

fn connector_name_from_config(package_cmd: PackageCmd, config: PathBuf) -> Result<String> {
    let opt = package_cmd.as_opt();
    let package_info = PackageInfo::from_options(&opt)?;

    let (_executable, metadata) = from_cargo_package(&package_info)
        .context("Failed to extract metadata from Connector.toml")?;

    let config_file = match std::fs::File::open(&config) {
        Ok(file) => file,
        Err(err) => {
            return Err(err).with_context(|| {
                format!("Could not open connector config at: {}", config.display())
            })
        }
    };

    let config = metadata.validate_config(config_file)?;

    Ok(config.meta().name().to_owned())
}

pub(crate) fn from_cargo_package(
    package_info: &PackageInfo,
) -> Result<(PathBuf, ConnectorMetadata)> {
    debug!("reading connector metadata from cargo package");

    let connector_metadata = ConnectorMetadata::from_toml_file(
        package_info.package_relative_path(CONNECTOR_METADATA_FILE_NAME),
    )?;
    let executable_path = package_info.target_bin_path()?;
    Ok((executable_path, connector_metadata))
}

fn from_ipkg_file(ipkg_file: PathBuf) -> Result<(PathBuf, ConnectorMetadata)> {
    println!("... checking package");
    debug!(
        "reading connector metadata from ipkg file {}",
        ipkg_file.to_string_lossy()
    );
    let package_meta = fluvio_hub_util::package_get_meta(ipkg_file.to_string_lossy().as_ref())
        .context("Failed to read package metadata")?;
    let entries: Vec<&Path> = package_meta.manifest.iter().map(Path::new).collect();

    let connector_toml = entries
        .iter()
        .find(|e| {
            e.file_name()
                .eq(&Some(OsStr::new(CONNECTOR_METADATA_FILE_NAME)))
        })
        .ok_or_else(|| anyhow!("Package missing {} file", CONNECTOR_METADATA_FILE_NAME))?;
    let connector_toml_bytes =
        fluvio_hub_util::package_get_manifest_file(&ipkg_file, connector_toml)?;
    let connector_metadata = ConnectorMetadata::from_toml_slice(&connector_toml_bytes)?;
    trace!("{:#?}", connector_metadata);

    let binary_name = connector_metadata
        .deployment
        .binary
        .as_ref()
        .ok_or_else(|| anyhow!("Only binary deployments are supported at this moment"))?;
    let binary = entries
        .iter()
        .find(|e| e.file_name().eq(&Some(OsStr::new(&binary_name))))
        .ok_or_else(|| anyhow!("Package missing {} file", binary_name))?;

    let binary_bytes = fluvio_hub_util::package_get_manifest_file(&ipkg_file, binary)?;
    let mut executable_path = TempDir::with_prefix("cdk-deploy-")
        .map_err(|e| anyhow!("Couldn't create tmpdir for cdk: {e}"))?
        .into_path();
    debug!(exe_tmp_dir=?executable_path, "ipkg temp dir");
    executable_path.push(binary_name);
    let mut file = File::create(&executable_path)?;
    set_exec_permissions(&mut file)?;
    file.write_all(&binary_bytes)?;

    Ok((executable_path, connector_metadata))
}

#[cfg(unix)]
fn set_exec_permissions(f: &mut File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    f.set_permissions(Permissions::from_mode(0o744))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_exec_permissions(f: &mut File) -> Result<()> {
    Ok(())
}

mod local_index {

    use std::{
        path::{PathBuf, Path},
        fmt::Display,
        io::Write,
    };
    use comfy_table::Table;
    use serde::{Serialize, Deserialize};

    use anyhow::{anyhow, Result};
    use fluvio_connector_deployer::DeploymentResult;
    use sysinfo::Pid;
    use tracing::debug;

    const LOCAL_INDEX_FILE_NAME: &str = "fluvio_cdk_deploy_index.toml";
    const LIST_TABLE_HEADERS: [&str; 2] = ["NAME", "STATUS"];

    #[derive(Debug, Serialize, Deserialize, Default)]
    struct LocalIndex<T: ConnectorOperator> {
        entries: Vec<Entry>,
        #[serde(skip)]
        path: PathBuf,
        #[serde(skip)]
        operator: T,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum Entry {
        Local {
            process_id: u32,
            name: String,
            log_file: Option<PathBuf>,
            tmp_dir: Option<PathBuf>,
        },
    }

    enum ConnectorStatus {
        Running,
        Stopped,
    }

    trait ConnectorOperator: Default {
        fn status(&self, entry: &Entry) -> Result<ConnectorStatus>;

        fn kill(&self, entry: &Entry) -> Result<()>;
    }

    struct LocalProcesses {
        system: sysinfo::System,
    }

    impl<T: ConnectorOperator> LocalIndex<T> {
        fn load<P: AsRef<Path>>(index_path: P) -> Result<Self> {
            let index_path = index_path.as_ref();
            debug!(?index_path, "loading");
            let mut index: Self =
                match std::fs::read_to_string(index_path).map(|content| toml::from_str(&content)) {
                    Ok(Ok(index)) => index,
                    Ok(Err(err)) => {
                        debug!(?err, "index file parsing failed");
                        Default::default()
                    }
                    Err(err) => {
                        debug!(?err, "index file read failed");
                        Default::default()
                    }
                };
            index_path.clone_into(&mut index.path);
            Ok(index)
        }

        fn insert(&mut self, entry: Entry) {
            self.entries.push(entry)
        }

        fn remove(&mut self, index: usize) -> Result<()> {
            let entry = self.entries.remove(index);
            self.operator.kill(&entry)?;
            match entry {
                Entry::Local {
                    process_id: _,
                    name: _,
                    log_file: _,
                    tmp_dir: Some(ref tmp_dir),
                } => {
                    // clean up tmp dir used with ipkg
                    std::fs::remove_dir_all(tmp_dir).map_err(|e| {
                        anyhow!("could not clean up ipkg dir {}: {e}", tmp_dir.display())
                    })
                }
                _ => Ok(()),
            }
        }

        fn find_by_name(&self, connector_name: &str) -> Option<(usize, &Entry)> {
            self.entries.iter().enumerate().find(|(_, entry)| {
                let Entry::Local {
                    process_id: _,
                    name,
                    log_file: _,
                    tmp_dir: _,
                } = entry;
                name.eq(connector_name)
            })
        }

        fn flush(&mut self) -> Result<()> {
            let index_path = &self.path;
            debug!(?index_path, "flushing");
            let content = toml::to_string(self)?;
            Ok(std::fs::write(index_path, content)?)
        }

        fn print_table<W: Write>(self, mut writer: W) -> Result<()> {
            if self.entries.is_empty() {
                writeln!(writer, "No connectors found")?;
                return Ok(());
            }

            let mut table = Table::new();
            table.load_preset(comfy_table::presets::NOTHING);
            table.set_header(LIST_TABLE_HEADERS);

            sysinfo::set_open_files_limit(0);
            let mut system = sysinfo::System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All);
            for connector in self.entries {
                let status = self.operator.status(&connector)?;
                let Entry::Local {
                    process_id: _,
                    name,
                    log_file: _,
                    tmp_dir: _,
                } = connector;
                table.add_row(vec![name, status.to_string()]);
            }
            writeln!(writer, "{table}")?;
            Ok(())
        }
    }

    impl ConnectorOperator for LocalProcesses {
        fn status(&self, entry: &Entry) -> Result<ConnectorStatus> {
            let Entry::Local {
                process_id,
                name: _,
                log_file: _,
                tmp_dir: _,
            } = entry;
            let status = if self.system.process(Pid::from_u32(*process_id)).is_some() {
                ConnectorStatus::Running
            } else {
                ConnectorStatus::Stopped
            };
            Ok(status)
        }

        fn kill(&self, entry: &Entry) -> Result<()> {
            let Entry::Local {
                process_id,
                name: _,
                log_file: _,
                tmp_dir: _,
            } = entry;

            if let Some(process) = self.system.process(Pid::from_u32(*process_id)) {
                process.kill();
            }

            Ok(())
        }
    }

    impl Default for LocalProcesses {
        fn default() -> Self {
            let mut system: sysinfo::System = Default::default();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All);
            Self { system }
        }
    }

    impl From<DeploymentResult> for Entry {
        fn from(value: DeploymentResult) -> Self {
            match value {
                DeploymentResult::Local {
                    process_id,
                    name,
                    log_file,
                    tmp_dir,
                } => Entry::Local {
                    process_id,
                    name,
                    log_file,
                    tmp_dir,
                },
            }
        }
    }

    impl Display for ConnectorStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let str = match self {
                Self::Running => "Running",
                Self::Stopped => "Stopped",
            };
            write!(f, "{str}")
        }
    }

    pub(super) fn store(deployment: DeploymentResult) -> Result<()> {
        let mut index = load()?;
        index.insert(deployment.into());
        index.flush()
    }

    pub(super) fn print() -> Result<()> {
        let index = load()?;
        index.print_table(std::io::stdout())
    }

    pub(super) fn delete_by_name(connector_name: &str) -> Result<()> {
        let mut index = load()?;

        match index.find_by_name(connector_name) {
            Some((
                i,
                Entry::Local {
                    process_id,
                    name,
                    log_file,
                    tmp_dir: _,
                },
            )) => {
                let log_file = match log_file {
                    Some(path) => format!("{}", path.display()),
                    None => "Not found".to_string(),
                };

                println!(
                    "Shutting down connector: {} \
                    \npid: {} \
                    \nLog File: {}",
                    name, process_id, log_file
                );
                index.remove(i)?;
            }
            None => println!("Connector not found: {}", connector_name),
        }

        index.flush()
    }

    pub(super) fn print_log(connector_name: &str) -> Result<()> {
        let index = load()?;
        if let Some((
            _,
            Entry::Local {
                process_id: _,
                name: _,
                log_file: Some(log_file),
                tmp_dir: _,
            },
        )) = index.find_by_name(connector_name)
        {
            let mut buf_reader = std::io::BufReader::new(std::fs::File::open(log_file)?);
            std::io::copy(&mut buf_reader, &mut std::io::stdout())?;
        };

        Ok(())
    }

    fn load() -> Result<LocalIndex<LocalProcesses>> {
        let mut index_path = std::env::temp_dir();
        index_path.push(LOCAL_INDEX_FILE_NAME);
        LocalIndex::load(index_path)
    }

    #[cfg(test)]
    mod tests {
        use std::{io::Cursor, time::SystemTime};

        use super::*;

        #[test]
        fn test_load_from_non_existing_file() -> Result<()> {
            //given
            let file_path = "non_existing_file";

            //when
            let index: LocalIndex<NoopOperator> = LocalIndex::load(file_path)?;

            let mut output = Cursor::new(Vec::new());
            index.print_table(&mut output)?;
            let output = String::from_utf8_lossy(output.get_ref());

            //then
            assert_eq!(output, "No connectors found\n");

            Ok(())
        }

        #[test]
        fn test_load_from_empty_file() -> Result<()> {
            //given
            let file_path = TestFile::new();
            std::fs::write(&file_path, [])?;

            //when
            let result = LocalIndex::load(&file_path);
            let index: LocalIndex<NoopOperator> = result?;

            let mut output = Cursor::new(Vec::new());
            index.print_table(&mut output)?;
            let output = String::from_utf8_lossy(output.get_ref());

            //then
            assert_eq!(output, "No connectors found\n");

            Ok(())
        }

        #[test]
        fn test_load_empty_add_and_flush() -> Result<()> {
            //given
            let file_path = TestFile::new();

            //when
            let mut index: LocalIndex<NoopOperator> = LocalIndex::load(&file_path)?;
            index.insert(Entry::Local {
                process_id: 1,
                name: "test_connector".to_owned(),
                log_file: None,
                tmp_dir: None,
            });
            index.flush()?;

            //then
            let mut output = Cursor::new(Vec::new());
            index.print_table(&mut output)?;
            let output = String::from_utf8_lossy(output.get_ref());

            assert_eq!(
                output,
                " NAME            STATUS  \n test_connector  Running \n"
            );

            assert_eq!(
                std::fs::read_to_string(file_path)?,
                "[[entries]]\ntype = \"local\"\nprocess_id = 1\nname = \"test_connector\"\n"
            );

            Ok(())
        }

        #[test]
        fn test_load_add_and_flush() -> Result<()> {
            //given
            let file_path = TestFile::new();
            std::fs::write(
                &file_path,
                b"[[entries]]\ntype = \"local\"\nprocess_id = 1\nname = \"test_connector\"\n",
            )?;

            //when
            let mut index: LocalIndex<NoopOperator> = LocalIndex::load(&file_path)?;
            assert!(index.find_by_name("test_connector").is_some());

            index.insert(Entry::Local {
                process_id: 2,
                name: "test_connector2".to_owned(),
                log_file: None,
                tmp_dir: None,
            });
            index.flush()?;

            //then
            let mut output = Cursor::new(Vec::new());
            index.print_table(&mut output)?;
            let output = String::from_utf8_lossy(output.get_ref());

            assert_eq!(
                output,
                " NAME             STATUS  \n test_connector   Running \n test_connector2  Running \n"
            );

            assert_eq!(
                std::fs::read_to_string(file_path)?,
                "[[entries]]\ntype = \"local\"\nprocess_id = 1\nname = \"test_connector\"\n\n[[entries]]\ntype = \"local\"\nprocess_id = 2\nname = \"test_connector2\"\n"
            );

            Ok(())
        }

        #[test]
        fn test_remove() -> Result<()> {
            //given
            let file_path = TestFile::new();
            std::fs::write(
                &file_path,
                b"[[entries]]\ntype = \"local\"\nprocess_id = 1\nname = \"test_connector\"\n\n[[entries]]\ntype = \"local\"\nprocess_id = 2\nname = \"test_connector2\"\n",
            )?;

            //when
            let mut index: LocalIndex<NoopOperator> = LocalIndex::load(&file_path)?;
            assert!(index.find_by_name("test_connector2").is_some());
            let (i, _) = index
                .find_by_name("test_connector")
                .expect("connector not found");

            index.remove(i)?;
            assert!(index.find_by_name("test_connector").is_none());

            index.flush()?;

            //then
            let mut output = Cursor::new(Vec::new());
            index.print_table(&mut output)?;
            let output = String::from_utf8_lossy(output.get_ref());

            assert_eq!(
                output,
                " NAME             STATUS  \n test_connector2  Running \n"
            );

            assert_eq!(
                std::fs::read_to_string(file_path)?,
                "[[entries]]\ntype = \"local\"\nprocess_id = 2\nname = \"test_connector2\"\n"
            );

            Ok(())
        }

        #[derive(Default)]
        struct NoopOperator;

        impl ConnectorOperator for NoopOperator {
            fn status(&self, _entry: &Entry) -> Result<ConnectorStatus> {
                Ok(ConnectorStatus::Running)
            }

            fn kill(&self, _entry: &Entry) -> Result<()> {
                Ok(())
            }
        }

        struct TestFile(PathBuf);

        impl TestFile {
            fn new() -> Self {
                let mut file_path = std::env::temp_dir();
                file_path.push(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("system time broken")
                        .as_nanos()
                        .to_string(),
                );
                Self(file_path)
            }
        }

        impl Drop for TestFile {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }

        impl AsRef<Path> for TestFile {
            fn as_ref(&self) -> &Path {
                self.0.as_ref()
            }
        }
    }
}
