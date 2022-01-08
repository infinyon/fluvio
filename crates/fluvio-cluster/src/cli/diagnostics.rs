use std::path::Path;
use std::fs::{copy, write};

use structopt::StructOpt;
use duct::cmd;
use which::which;

use fluvio::config::ConfigFile;

use crate::cli::ClusterCliError;
use crate::cli::start::get_log_directory;

type Result<T, E = ClusterCliError> = core::result::Result<T, E>;

#[derive(StructOpt, Debug)]
pub struct DiagnosticsOpt {}

impl DiagnosticsOpt {
    pub async fn process(self) -> Result<()> {
        let config = ConfigFile::load_default_or_new()?;
        let temp_dir = tempdir::TempDir::new("fluvio-diagnostics")?;
        let temp_path = temp_dir.path();

        match config.config().current_profile_name() {
            // Local cluster
            Some("local") => {
                self.copy_local_logs(temp_path)?;
            }
            // Cloud cluster
            Some(other) if other.contains("cloud") => {
                println!("Cannot collect logs from Cloud, skipping");
            }
            // Guess Kubernetes cluster
            _ => {
                let kubectl = match which("kubectl") {
                    Ok(kubectl) => kubectl,
                    Err(_) => {
                        println!("Missing `kubectl`, needed for collecting logs");
                        return Ok(());
                    }
                };

                let _ = self.copy_kubernetes_logs(&kubectl, temp_path);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "pod", true);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "pvc", true);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "service", true);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "statefulset", true);

                // Fluvio CRDs
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "spu", false);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "topic", false);
                let _ = self.copy_kubernetes_metadata(&kubectl, temp_path, "partition", false);
            }
        }
        let _ = self.copy_fluvio_specs(temp_path).await;
        self.basic_diagnostics(temp_path)?;

        let time = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let diagnostic_path = std::env::current_dir()?.join(format!("diagnostics-{}.tar.gz", time));
        let mut diagnostic_file = std::fs::File::create(&diagnostic_path)?;
        self.zip_files(temp_path, &mut diagnostic_file)
            .map_err(|e| ClusterCliError::Other(format!("failed to zip diagnostics: {}", e)))?;

        println!("Wrote diagnostics to {}", diagnostic_path.display());
        Ok(())
    }

    fn zip_files(&self, source: &Path, output: &mut std::fs::File) -> Result<(), std::io::Error> {
        use flate2::write::GzEncoder;

        let mut gzipper = GzEncoder::new(output, flate2::Compression::default());
        {
            let mut archive = tar::Builder::new(&mut gzipper);
            archive.append_dir_all("diagnostics", source)?;
        }
        gzipper.finish()?;
        Ok(())
    }

    fn copy_local_logs(&self, dest_dir: &Path) -> Result<()> {
        let logs_dir = std::fs::read_dir(get_log_directory())?;

        for entry in logs_dir.flat_map(|it| it.ok()) {
            let to = dest_dir.join(entry.file_name());
            if entry.file_name() == "flv_sc.log" {
                copy(entry.path(), &to)?;
            }
            if entry.file_name().to_string_lossy().starts_with("spu_log") {
                copy(entry.path(), &to)?;
            }
        }
        Ok(())
    }

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
            .filter(|pod| pod.contains("fluvio"))
            .collect::<Vec<_>>();

        for &pod in &pods {
            let log_result = cmd!(kubectl, "logs", pod).stderr_capture().read();
            let log = match log_result {
                Ok(log) => log,
                Err(_) => {
                    println!("Failed to collect log for {}, skipping", pod.trim());
                    continue;
                }
            };

            let dest_path = dest_dir.join(format!("pod-{}.log", pod));
            write(dest_path, log)?;
        }

        Ok(())
    }

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
            .filter(|obj| !filter_fluvio || obj.contains("fluvio"))
            .map(|name| name.trim())
            .collect::<Vec<_>>();

        for &obj in &objects {
            let result = cmd!(kubectl, "get", ty, obj, "-o", "yaml")
                .stderr_capture()
                .read();
            let meta = match result {
                Ok(meta) => meta,
                Err(_) => continue,
            };

            let dest = dest.join(format!("{}-{}.yaml", ty, obj));
            std::fs::write(dest, meta)?;
        }
        Ok(())
    }

    async fn copy_fluvio_specs(&self, dest: &Path) -> Result<()> {
        use fluvio::Fluvio;
        use fluvio::metadata::{
            topic::TopicSpec, partition::PartitionSpec, spu::SpuSpec, spg::SpuGroupSpec,
        };
        let fluvio = Fluvio::connect().await?;
        let admin = fluvio.admin().await;

        let write = |yaml, name| -> Result<()> {
            let path = dest.join(format!("admin-spec-{}.yml", name));
            std::fs::write(path, yaml)?;
            Ok(())
        };

        let topics = admin.list::<TopicSpec, _>([]).await?;
        let topics = serde_yaml::to_string(&topics).unwrap();
        write(&topics, "topics")?;

        let partitions = admin.list::<PartitionSpec, _>([]).await?;
        let partitions = serde_yaml::to_string(&partitions).unwrap();
        write(&partitions, "partitions")?;

        let spus = admin.list::<SpuSpec, _>([]).await?;
        let spus = serde_yaml::to_string(&spus).unwrap();
        write(&spus, "spus")?;

        let spgs = admin.list::<SpuGroupSpec, _>([]).await?;
        let spgs = serde_yaml::to_string(&spgs).unwrap();
        write(&spgs, "spgs")?;

        Ok(())
    }

    /// write helm and other basic stuff
    fn basic_diagnostics(&self, dest_dir: &Path) -> Result<()> {
        let path = dest_dir.join("helm-list.txt");
        match cmd!("helm", "list").read() {
            Ok(output) => {
                write(path, output)?;
            }
            Err(err) => {
                write(path, format!("Failed to collect helm list: {:#?}", err))?;
            }
        }

        Ok(())
    }
}
