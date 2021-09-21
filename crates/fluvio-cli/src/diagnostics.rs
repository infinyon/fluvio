use std::path::Path;
use structopt::StructOpt;
use fluvio::config::ConfigFile;
use duct::cmd;
use which::which;

use crate::{Result, CliError};

#[cfg(target_os = "macos")]
const LOGS_PATH: &str = "/usr/local/var/log/fluvio/";

#[cfg(not(target_os = "macos"))]
const LOGS_PATH: &str = "/tmp/";

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
                if which("kubectl").is_err() {
                    println!("Missing `kubectl`, needed for collecting logs");
                    return Ok(());
                }

                self.copy_kubernetes_logs(temp_path)?;
                self.copy_kubernetes_metadata(temp_path, "pod", true)?;
                self.copy_kubernetes_metadata(temp_path, "service", true)?;
                self.copy_kubernetes_metadata(temp_path, "statefulset", true)?;

                // Fluvio CRDs
                self.copy_kubernetes_metadata(temp_path, "spu", false)?;
                self.copy_kubernetes_metadata(temp_path, "topic", false)?;
                self.copy_kubernetes_metadata(temp_path, "partition", false)?;
            }
        }
        self.copy_fluvio_specs(temp_path).await?;

        let time = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let diagnostic_path = std::env::current_dir()?.join(format!("diagnostics-{}.zip", time));
        println!("Diagnostic path: {}", diagnostic_path.display());
        let mut diagnostic_file = std::fs::File::create(&diagnostic_path)?;
        self.zip_files(temp_path, &mut diagnostic_file)
            .map_err(|e| CliError::Other(format!("failed to zip diagnostics: {}", e)))?;

        println!("Copied logs to {}", temp_path.display());
        std::thread::sleep(std::time::Duration::from_secs(60));
        Ok(())
    }

    fn zip_files(
        &self,
        source: &Path,
        output: &mut std::fs::File,
    ) -> Result<(), zip::result::ZipError> {
        use std::io::Write;
        use zip::{ZipWriter, write::FileOptions};

        let mut zipper = ZipWriter::new(output);
        let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored);

        let dir = std::fs::read_dir(source)?;
        for result in dir {
            let entry = match result {
                Ok(entry) => entry,
                Err(e) => {
                    println!("Failed to zip file, skipping: {}", e);
                    continue;
                }
            };

            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let contents = std::fs::read(path)?;
            zipper.start_file(name, options)?;
            zipper.write(&contents)?;
        }

        zipper.finish()?;
        Ok(())
    }

    fn copy_local_logs(&self, dest_dir: &Path) -> Result<()> {
        let logs_dir = std::fs::read_dir(LOGS_PATH)?;

        for entry in logs_dir.flat_map(|it| it.ok()) {
            let to = dest_dir.join(entry.file_name());
            if entry.file_name() == "flv_sc.log" {
                std::fs::copy(entry.path(), &to)?;
            }
            if entry.file_name().to_string_lossy().starts_with("spu_log") {
                std::fs::copy(entry.path(), &to)?;
            }
        }
        Ok(())
    }

    fn copy_kubernetes_logs(&self, dest_dir: &Path) -> Result<()> {
        let kubectl = which("kubectl")?;
        let pods = cmd!(
            &kubectl,
            "get",
            "pods",
            "-o",
            "jsonpath={.items[*].metadata.name}"
        )
        .read()?;
        // Filter for only Fluvio pods
        let pods = pods
            .split(" ")
            .filter(|pod| pod.starts_with("fluvio"))
            .collect::<Vec<_>>();

        for &pod in &pods {
            let log_result = cmd!(&kubectl, "logs", pod).read();
            let log = match log_result {
                Ok(log) => log,
                Err(e) => {
                    println!("Failed to collect log for {}, skipping: {}", pod, e);
                    continue;
                }
            };

            let dest_path = dest_dir.join(format!("pod-{}.log", pod));
            println!("Writing log to {}", dest_path.display());
            std::fs::write(dest_path, log)?;
        }

        Ok(())
    }

    fn copy_kubernetes_metadata(&self, dest: &Path, ty: &str, filter_fluvio: bool) -> Result<()> {
        let kubectl = which("kubectl")?;
        let objects = cmd!(
            &kubectl,
            "get",
            ty,
            "-o",
            "jsonpath={.items[*].metadata.name}"
        )
        .read()?;
        // Filter for only Fluvio services
        let objects = objects
            .split(" ")
            .filter(|obj| !filter_fluvio || obj.starts_with("fluvio"))
            .collect::<Vec<_>>();

        for &obj in &objects {
            let result = cmd!(&kubectl, "get", ty, obj, "-o", "yaml").read();
            let meta = match result {
                Ok(meta) => meta,
                Err(e) => {
                    println!(
                        "Failed to collect metadata for {} {}, skipping: {}",
                        ty, obj, e
                    );
                    continue;
                }
            };

            let dest = dest.join(format!("{}-{}.yaml", ty, obj));
            println!("Writing metadata to {}", dest.display());
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
}
