use std::path::{PathBuf, Path};
use structopt::StructOpt;
use color_eyre::Result;
use duct::cmd;

use crate::build::BuildOpt;
use crate::dockerfile_path;

#[derive(StructOpt, Debug, Default)]
pub struct DockerOpt {
    #[structopt(flatten)]
    build: BuildOpt,
    /// Load the docker image into minikube after building
    #[structopt(long)]
    minikube: bool,
    /// Load the docker image into k3d after building
    #[structopt(long)]
    k3d: bool,
    /// Load the docker image into kind after building
    #[structopt(long)]
    kind: bool,
}

pub struct DockerBuildStatus {
    pub tag: String,
    pub targeted_tag: String,
    pub path: PathBuf,
}

impl DockerOpt {
    pub fn build_image(&self) -> Result<()> {
        let docker_status = self.build_docker_image()?;
        if self.minikube {
            println!("Loading into minikube");
            self.load_image_minikube(&docker_status.path)?;
        }
        if self.k3d {
            println!("Loading into k3d");
            self.load_image_k3d(&docker_status.path)?;
        }
        if self.kind {
            println!("Loading into kind");
            self.load_image_kind(&docker_status)?;
        }
        Ok(())
    }

    /// Build Fluvio's docker image wrapping `fluvio-run`, the server executable
    ///
    /// This task also exports the `infinyon/fluvio` docker image as a tarball
    /// adjacent to the `fluvio-run` executable that was built.
    fn build_docker_image(&self) -> Result<DockerBuildStatus> {
        const DOCKER_REPO: &str = "infinyon/fluvio";

        let tmp = tempdir::TempDir::new("fluvio-image")?;
        let git_hash = cmd!("git", "rev-parse", "HEAD").stdout_capture().read()?;
        let target = self.build.target.as_deref().unwrap_or(env!("BUILD_TARGET"));

        // Copy fluvio-run to tmp directory
        let fluvio_run = self.build.build_cluster()?;
        let fluvio_run_dir = PathBuf::from(fluvio_run.parent().unwrap());
        let tmp_bin_path = tmp.path().join("fluvio-run");
        std::fs::copy(&fluvio_run, &tmp_bin_path)?;

        // Copy Dockerfile to tmp directory
        let dockerfile = dockerfile_path()?;
        let tmp_dockerfile_path = tmp.path().join("Dockerfile");
        std::fs::copy(&dockerfile, &tmp_dockerfile_path)?;

        let docker_tag = format!("{repo}:{hash}", repo = DOCKER_REPO, hash = git_hash);
        let docker_tag_target = format!("{tag}-{target}", tag = docker_tag, target = target);
        let docker_arg = match target {
            "aarch64-unknown-linux-musl" => vec!["--build-arg", "ARCH=arm64v8/"],
            _ => vec![],
        };

        // Arguments for docker build. Vec<Vec> to flatten optional arguments
        let args: Vec<&str> = vec![
            vec!["build"],
            vec!["-t", &*docker_tag],        // -t infinyon/fluvio:hash
            vec!["-t", &*docker_tag_target], // -t infinyon/fluvio:hash-target
            docker_arg, // --build-arg ARCH=arm64v8/ IFF --target == aarch64-unknown-linux-musl
            vec!["."],
        ]
        .into_iter()
        .flatten()
        .collect();

        // Run docker build from within the tmp directory
        duct::cmd("docker", &args).dir(tmp.path()).run()?;

        // Save docker image to target/infinyon-fluvio.tar
        let docker_tar = fluvio_run_dir.join("infinyon-fluvio.tar");
        cmd!(
            "docker",
            "image",
            "save",
            &docker_tag,
            "--output",
            &docker_tar
        )
        .run()?;

        let status = DockerBuildStatus {
            tag: docker_tag,
            targeted_tag: docker_tag_target,
            path: docker_tar,
        };
        Ok(status)
    }

    /// Load the docker image tar from the given path into minikube's docker environment
    fn load_image_minikube(&self, tar: &Path) -> Result<()> {
        let infinyon_fluvio_tar = std::fs::File::open(tar)?;
        // Below is equivalent to the following:
        // eval $(minikube docker-env) && docker load < infinyon-fluvio.tar
        cmd!(
            "bash",
            "-c",
            "eval $(minikube -p minikube docker-env) && docker load"
        )
        .stdin_file(infinyon_fluvio_tar)
        .run()?;
        Ok(())
    }

    /// Load the docker image tar from the given path into k3d
    fn load_image_k3d(&self, tar: &Path) -> Result<()> {
        cmd!("k3d", "image", "import", "-k", tar).run()?;
        Ok(())
    }

    /// Load the docker image by name from the docker daemon into kind
    fn load_image_kind(&self, status: &DockerBuildStatus) -> Result<()> {
        let image_tag = &status.tag;
        cmd!("kind", "load", "docker-image", image_tag).run()?;
        Ok(())
    }
}
