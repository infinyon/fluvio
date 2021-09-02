use std::sync::Mutex;
use std::collections::HashSet;
use structopt::StructOpt;
use duct::cmd;
use which::which;
use color_eyre::Result;
use color_eyre::eyre::WrapErr;
use once_cell::sync::{OnceCell, Lazy};

const CARGO: &str = env!("CARGO");

#[derive(StructOpt, Debug)]
pub struct Root {
    #[structopt(subcommand)]
    pub cmd: RootCmd,
}

#[derive(StructOpt, Debug)]
pub enum RootCmd {
    Build(BuildOpt),
    Clippy(BuildOpt),
    Test(BuildOpt),
    #[structopt(aliases = &["test-unit", "unit-test", "unit-tests"])]
    TestUnits(BuildOpt),
    #[structopt(aliases = &["test-doc", "doc-test", "doc-tests"])]
    TestDocs(BuildOpt),
    TestClientDocs(BuildOpt),
    #[structopt(aliases = &["integration-test", "integration-tests"])]
    TestIntegration(BuildOpt),
    InstallTarget(InstallTargetOpt),
}

impl RootCmd {
    pub fn process(self) -> Result<()> {
        match self {
            Self::Build(opt) => {
                opt.build()?;
            }
            Self::Clippy(opt) => {
                opt.clippy()?;
            }
            Self::Test(opt) => {
                opt.test()?;
            }
            Self::TestDocs(opt) => {
                opt.test_docs()?;
            }
            Self::TestClientDocs(opt) => {
                opt.test_client_docs()?;
            }
            Self::TestUnits(opt) => {
                opt.test_units()?;
            }
            Self::TestIntegration(opt) => {
                opt.test_integration()?;
            }
            Self::InstallTarget(opt) => {
                opt.install_target()?;
            }
        }
        Ok(())
    }
}

#[derive(StructOpt, Debug, Default)]
pub struct BuildOpt {
    #[structopt(long)]
    release: bool,
    #[structopt(long)]
    verbose: bool,
    target: Option<String>,
}

impl BuildOpt {
    pub fn build(&self) -> Result<()> {
        println!("Building all artifacts");
        self.build_cli()?;
        self.build_cluster()?;
        self.build_test()?;
        self.build_smartstreams()?;
        Ok(())
    }

    pub fn build_cli(&self) -> Result<()> {
        install_target(None)?;
        println!("Building fluvio");
        cmd!(CARGO, "build", "--bin", "fluvio").run()?;
        Ok(())
    }

    pub fn build_cluster(&self) -> Result<()> {
        install_target(None)?;
        println!("Building fluvio-run");
        cmd!(CARGO, "build", "--bin", "fluvio-run").run()?;
        Ok(())
    }

    pub fn build_test(&self) -> Result<()> {
        install_target(None)?;
        println!("Building fluvio-test");
        cmd!(CARGO, "build", "--bin", "fluvio-test").run()?;
        Ok(())
    }

    pub fn build_smartstreams(&self) -> Result<()> {
        install_target(Some("wasm32-unknown-unknown"))?;
        cmd!(
            CARGO,
            "build",
            "--target=wasm32-unknown-unknown",
            "--manifest-path=crates/fluvio-smartstream/examples/Cargo.toml",
        )
        .run()?;
        Ok(())
    }

    pub fn test(&self) -> Result<()> {
        self.test_units()?;
        self.test_docs()?;
        self.test_integration()?;
        Ok(())
    }

    pub fn test_units(&self) -> Result<()> {
        cmd!(CARGO, "test", "--lib", "--all-features").run()?;
        Ok(())
    }

    pub fn test_docs(&self) -> Result<()> {
        cmd!(CARGO, "test", "--doc", "--all-features").run()?;
        Ok(())
    }

    pub fn test_client_docs(&self) -> Result<()> {
        cmd!(
            CARGO,
            "test",
            "--doc",
            "--all-features",
            "--package=fluvio",
            "--package=fluvio-cli",
            "--package=fluvio-cluster"
        )
        .run()?;
        Ok(())
    }

    pub fn test_integration(&self) -> Result<()> {
        cmd!(
            CARGO,
            "test",
            "--lib",
            "--all-features",
            "--",
            "--ignored",
            "--test-threads=1"
        )
        .run()?;
        Ok(())
    }

    pub fn clippy(&self) -> Result<()> {
        println!("Checking clippy");
        // Use `cargo check` first to leverage any caching
        cmd!(CARGO, "check", "--all", "--all-features", "--tests").run()?;
        cmd!(
            CARGO,
            "clippy",
            "--all",
            "--all-features",
            "--tests",
            "--",
            "-D",
            "warnings",
            "-A",
            "clippy::upper_case_acronyms"
        )
        .run()?;
        Ok(())
    }
}

#[derive(StructOpt, Debug)]
pub struct InstallTargetOpt {
    target: Option<String>,
}

impl InstallTargetOpt {
    pub fn install_target(&self) -> Result<()> {
        install_target(self.target.as_deref())?;
        Ok(())
    }
}

/// Installs `cross` or runs `rustup target add` as needed for the given target.
pub fn install_target(target: Option<&str>) -> Result<()> {
    let target = target.unwrap_or(env!("BUILD_TARGET"));

    {
        // In a given task, we only ever want to run install_target
        // once per unique target.
        static TARGETS: Lazy<Mutex<HashSet<String>>> = Lazy::new(|| Mutex::new(HashSet::new()));
        let mut ts = TARGETS.lock().unwrap();
        if ts.contains(target) {
            return Ok(());
        } else {
            ts.insert(target.to_string());
        }
    }

    match target {
        "armv7-unknown-linux-gnueabihf" | "arm-unknown-linux-gnueabihf" => {
            install_cross()?;
        }
        _ => {
            rustup_target_add(target)?;
        }
    }
    Ok(())
}

/// Runs `rustup target add` with the provided target
fn rustup_target_add(target: &str) -> Result<()> {
    let rustup = which("rustup").wrap_err("failed to find rustup")?;
    println!("Adding rustup target for {}", target);
    cmd!(rustup, "target", "add", target).run()?;
    Ok(())
}

/// Installs cross using `cargo install`
fn install_cross() -> Result<()> {
    // Run at most one time
    static INSTALLED_CROSS: OnceCell<()> = OnceCell::new();
    if INSTALLED_CROSS.set(()).is_err() {
        return Ok(());
    }
    print!("Installing cross... ");
    if is_cross_installed() {
        println!("already installed");
        return Ok(());
    }
    cmd!(CARGO, "install", "cross").run()?;
    println!("ok");
    Ok(())
}

/// Checks whether `cross` is installed
fn is_cross_installed() -> bool {
    which("cross").is_ok()
}
