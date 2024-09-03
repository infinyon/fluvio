use std::path::{Path, PathBuf};
use std::fs::{copy, write};
use std::io::Error as IoError;

use clap::Parser;
use serde::Serialize;
use duct::cmd;
use sysinfo::{System, Networks, Disks};
use which::which;
use anyhow::Result;

use fluvio::metadata::{topic::TopicSpec, partition::PartitionSpec, spg::SpuGroupSpec, spu::SpuSpec};
use fluvio_sc_schema::objects::Metadata;

use crate::{InstallationType, cli::get_installation_type};
use crate::cli::ClusterCliError;
use crate::cli::start::default_log_directory;
use crate::start::local::{DEFAULT_DATA_DIR as DEFAULT_LOCAL_DIR, DEFAULT_METADATA_SUB_DIR};

const FLUVIO_PROCESS_NAME: &str = "fluvio";

#[derive(Parser, Debug)]
pub struct DiagnosticsOpt {
    #[arg(long)]
    quiet: bool,
}

impl DiagnosticsOpt {
    pub async fn process(self) -> Result<()> {
        let (installation_ty, config) = get_installation_type()?;
        let profile = config.config().current_profile_name().unwrap_or("none");
        println!("Installation type: {installation_ty:#?}\nProfile: {profile}");
        let temp_dir = tempfile::Builder::new()
            .prefix("fluvio-diagnostics")
            .tempdir()?;
        let temp_path = temp_dir.path();

        let spu_specs = match self.copy_fluvio_specs(temp_path).await {
            Ok(specs) => specs,
            Err(err) => {
                eprintln!("error copying fluvio specs: {err:#?}");
                vec![]
            }
        };

        // write internal fluvio cluster internal state
        match installation_ty {
            // Local cluster
            InstallationType::Local | InstallationType::ReadOnly => {
                self.copy_local_logs(temp_path)?;
                self.copy_local_metadata(temp_path)?;
                for spu in spu_specs {
                    self.spu_disk_usage(None, temp_path, &spu.spec)?;
                }
            }
            // Local cluster with k8 metadata
            InstallationType::LocalK8 => {
                self.write_helm(temp_path)?;
                self.copy_local_logs(temp_path)?;
                for spu in spu_specs {
                    self.spu_disk_usage(None, temp_path, &spu.spec)?;
                }
            }
            // Kubernetes cluster
            InstallationType::K8 => {
                let kubectl = match which("kubectl") {
                    Ok(kubectl) => kubectl,
                    Err(_) => {
                        println!("Missing `kubectl`, needed for collecting logs");
                        return Ok(());
                    }
                };

                self.write_helm(temp_path)?;
                let _ = self.copy_kubernetes_logs(&kubectl, temp_path);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "pod", true);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "pvc", true);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "service", true);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "statefulset", true);

                // Fluvio CRDs
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "spu", false);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "topic", false);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "partition", false);

                for spu in spu_specs {
                    self.spu_disk_usage(Some(&kubectl), temp_path, &spu.spec)?;
                }
            }
            _other => {}
        }

        self.write_system_info(temp_path)?;

        let time = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let diagnostic_path = std::env::current_dir()?.join(format!("diagnostics-{time}.tar.gz"));
        let mut diagnostic_file = std::fs::File::create(&diagnostic_path)?;
        self.zip_files(temp_path, &mut diagnostic_file)
            .map_err(|e| ClusterCliError::Other(format!("failed to zip diagnostics: {e}")))?;

        println!("Wrote diagnostics to {}", diagnostic_path.display());
        Ok(())
    }

    fn zip_files(&self, source: &Path, output: &mut std::fs::File) -> Result<()> {
        use flate2::write::GzEncoder;

        let mut gzipper = GzEncoder::new(output, flate2::Compression::default());
        {
            let mut archive = tar::Builder::new(&mut gzipper);
            archive.append_dir_all("diagnostics", source)?;
        }
        gzipper.finish()?;
        Ok(())
    }

    // copy logs from spu
    fn copy_local_logs(&self, dest_dir: &Path) -> Result<()> {
        let logs_dir = std::fs::read_dir(default_log_directory())?;
        println!("reading local logs from {logs_dir:?}");
        for entry in logs_dir.flat_map(|it| it.ok()) {
            let to = dest_dir.join(entry.file_name());
            let file_name = entry.file_name();
            if file_name == "flv_sc.log" || file_name.to_string_lossy().starts_with("spu_log") {
                println!("copying local log file: {:?}", entry.path());
                copy(entry.path(), to)?;
            } else {
                println!("skipping {file_name:?}");
            }
        }
        Ok(())
    }

    fn copy_local_metadata(&self, dest_dir: &Path) -> Result<()> {
        let metadata_path = DEFAULT_LOCAL_DIR
            .to_owned()
            .unwrap_or_default()
            .join(DEFAULT_METADATA_SUB_DIR);
        if metadata_path.exists() {
            println!("reading local metadata from {metadata_path:?}");
            let mut metadata_file = std::fs::File::create(dest_dir.join("metadata.tar.gz"))?;
            self.zip_files(&metadata_path, &mut metadata_file)?;
        }
        Ok(())
    }

    /// get logs from k8 pod
    fn copy_kubernetes_logs(&self, kubectl: &Path, dest_dir: &Path) -> Result<()> {
        let pods = cmd!(
            kubectl,
            "get",
            "pods",
            "-o",
            "jsonpath={.items[*].metadata.name}"
        )
        .read()?;
        // Filter for only Fluvio pods
        let pods = pods
            .split(' ')
            .filter(|pod| pod.contains(FLUVIO_PROCESS_NAME))
            .collect::<Vec<_>>();

        for &pod in &pods {
            let log_result = cmd!(kubectl, "logs", pod).stderr_capture().read();
            let log = match log_result {
                Ok(log) => log,
                Err(_) => {
                    if !self.quiet {
                        println!("Failed to collect log for {}, skipping", pod.trim());
                    }
                    continue;
                }
            };

            let dest_path = dest_dir.join(format!("pod-{pod}.log"));
            write(dest_path, log)?;
        }

        Ok(())
    }

    /// get detail about k8 object
    fn copy_kubernetes_metadata(
        &self,
        kubectl: &Path,
        dest: &Path,
        ty: &str,
        filter_fluvio: bool,
    ) -> Result<()> {
        let objects = cmd!(
            kubectl,
            "get",
            ty,
            "-o",
            "jsonpath={.items[*].metadata.name}"
        )
        .read()?;
        // Filter for only Fluvio services
        let objects = objects
            .split(' ')
            .filter(|obj| !filter_fluvio || obj.contains(FLUVIO_PROCESS_NAME))
            .map(|name| name.trim())
            .collect::<Vec<_>>();

        for &obj in &objects {
            println!("getting k8: {ty} {obj}");
            let result = cmd!(kubectl, "get", ty, obj, "-o", "yaml")
                .stderr_capture()
                .read();
            let dest = dest.join(format!("{ty}-{obj}.yaml"));
            self.dump(&format!("k8: {ty}"), dest, result)?;
        }
        Ok(())
    }

    async fn copy_fluvio_specs(&self, dest: &Path) -> Result<Vec<Metadata<SpuSpec>>> {
        use fluvio::Fluvio;

        println!("start copying fluvio specs from...");
        let fluvio = Fluvio::connect().await?;
        let admin = fluvio.admin().await;

        let write_spec = |yaml, name| -> Result<()> {
            let path = dest.join(format!("admin-spec-{name}.yml"));
            self.dump(name, path, Ok(yaml))?;
            Ok(())
        };

        println!("getting topic spec");
        let topics = admin.all::<TopicSpec>().await?;
        let topics = serde_yaml::to_string(&topics).unwrap();
        write_spec(topics, "topics")?;

        println!("getting partition spec");
        let partitions = admin.all::<PartitionSpec>().await?;
        let partitions = serde_yaml::to_string(&partitions).unwrap();
        write_spec(partitions, "partitions")?;

        println!("getting spu spec");
        let spus = admin.all::<SpuSpec>().await?;
        let spu_description = serde_yaml::to_string(&spus).unwrap();
        write_spec(spu_description, "spus")?;

        println!("getting spg spec");
        let spgs = admin.all::<SpuGroupSpec>().await?;
        let spgs = serde_yaml::to_string(&spgs).unwrap();
        write_spec(spgs, "spgs")?;

        Ok(spus)
    }

    /// write helm and other basic stuff
    fn write_helm(&self, dest_dir: &Path) -> Result<()> {
        let path = dest_dir.join("helm-list.txt");
        println!("getting helm list");
        self.dump("helm list", path, cmd!("helm", "list").read())?;
        Ok(())
    }

    fn write_system_info(&self, dest: &Path) -> Result<()> {
        let write = |yaml, name| -> Result<()> {
            let path = dest.join(format!("system-{name}.yml"));
            self.dump(name, path, Ok(yaml))?;
            Ok(())
        };

        sysinfo::set_open_files_limit(0);
        let mut sys = System::new_all();
        let mut net = Networks::new();

        // First we update all information of our `System` struct.
        println!("getting system info");
        sys.refresh_all();
        net.refresh();

        let info = SystemInfo::load(&sys);
        write(serde_yaml::to_string(&info).unwrap(), "sysinfo")?;

        //let disks = DiskInfo::load(&sys);
        //let disk_string = serde_yaml::to_string(&disks).unwrap();
        //println!("{}", disk_string);
        //  write(&disk_string, "disk")?;

        let networks = NetworkInfo::load(&net);
        write(serde_yaml::to_string(&networks).unwrap(), "networks")?;

        let processes = ProcessInfo::load(&sys);
        write(serde_yaml::to_string(&processes).unwrap(), "processes")?;

        Ok(())
    }

    /// find disk usage
    fn spu_disk_usage(
        &self,
        kubectl: Option<&PathBuf>,
        dest: &Path,
        spu_spec: &SpuSpec,
    ) -> Result<()> {
        let spu = spu_spec.id;
        let ls_cmd = match kubectl {
            Some(kct) => {
                let pod_id = format!("fluvio-spg-main-{spu}");
                println!("retrieved k8 spu disk log {pod_id}");
                cmd!(
                    kct,
                    "exec",
                    pod_id,
                    "--",
                    "ls",
                    "-lh",
                    "-R",
                    format!("/var/lib/fluvio/data/spu-logs-{spu}/")
                )
            }
            None => {
                let log_dir = (DEFAULT_LOCAL_DIR.to_owned())
                    .unwrap()
                    .join(format!("spu-logs-{spu}"));
                println!("retrieved local spu disk log {log_dir:?}");
                cmd!("ls", "-lh", "-R", log_dir)
            }
        };

        let result = ls_cmd.stderr_capture().read();
        let dest = dest.join(format!("{spu}-disk.log"));
        self.dump(&format!("spu disk: {spu}"), dest, result)?;

        Ok(())
    }

    fn dump<P: AsRef<Path>>(
        &self,
        label: &str,
        path: P,
        contents: Result<String, IoError>,
    ) -> Result<()> {
        if !self.quiet {
            println!("---------");
        }
        match contents {
            Ok(output) => {
                if !self.quiet {
                    println!("{}", &output);
                }
                write(path, output)?;
            }
            Err(err) => {
                let output_err = format!("Failed to collect {label} list: {err:#?}");
                if !self.quiet {
                    println!("{output_err}");
                }
                write(path, &output_err)?;
            }
        }

        if !self.quiet {
            println!("---------");
            println!();
            println!();
        }

        Ok(())
    }
}

#[derive(Serialize)]
struct SystemInfo {
    name: String,
    kernel_version: String,
    os_version: String,
    host_name: String,
    processors: usize,
    total_memory: u64,
    total_swap: u64,
    used_swap: u64,
}

impl SystemInfo {
    fn load(sys: &System) -> Self {
        Self {
            name: System::name().unwrap_or_default(),
            kernel_version: System::kernel_version().unwrap_or_default(),
            os_version: System::os_version().unwrap_or_default(),
            host_name: System::host_name().unwrap_or_default(),
            processors: sys.cpus().len(),
            total_memory: sys.total_memory(),
            total_swap: sys.total_swap(),
            used_swap: sys.used_swap(),
        }
    }
}

#[allow(dead_code)]
#[derive(Serialize)]
struct DiskInfo {
    name: String,
    mount_point: String,
    space: u64,
    available: u64,
    file_system: String,
}

impl DiskInfo {
    fn _load(diskinfo: &Disks) -> Vec<DiskInfo> {
        let mut disks = Vec::new();

        for disk in diskinfo {
            disks.push(DiskInfo {
                name: format!("{:?}", disk.name()),
                mount_point: format!("{:?}", disk.mount_point()),
                space: disk.total_space(),
                available: disk.available_space(),
                file_system: format!("{:?}", disk.file_system()),
            });
        }

        disks
    }
}

#[derive(Serialize)]
struct NetworkInfo {
    name: String,
    received: u64,
    transmitted: u64,
}

impl NetworkInfo {
    fn load(net: &Networks) -> Vec<NetworkInfo> {
        let mut networks = Vec::new();

        for network in net {
            networks.push(NetworkInfo {
                name: network.0.to_string(),
                received: network.1.received(),
                transmitted: network.1.transmitted(),
            });
        }

        networks
    }
}

#[derive(Serialize)]
struct ProcessInfo {
    pid: u32,
    name: String,
    disk_usage: String,
    cmd: String,
}

impl ProcessInfo {
    fn load(sys: &System) -> Vec<ProcessInfo> {
        let mut processes = Vec::new();

        for (pid, process) in sys.processes() {
            let process_name = process.name().to_str();

            if let Some(process_name) = process_name {
                if process_name.contains(FLUVIO_PROCESS_NAME) {
                    processes.push(ProcessInfo {
                        pid: pid.as_u32(),
                        name: process_name.to_string(),
                        disk_usage: format!("{:?}", process.disk_usage()),
                        cmd: format!("{:?}", process.cmd()),
                    });
                }
            }
        }

        processes
    }
}
