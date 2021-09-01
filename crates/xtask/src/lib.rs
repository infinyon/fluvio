use std::sync::Mutex;
use std::collections::HashSet;
use duct::cmd;
use which::which;
use color_eyre::Result;
use color_eyre::eyre::WrapErr;
use once_cell::sync::{OnceCell, Lazy};

const CARGO: &str = env!("CARGO");

pub fn build() -> Result<()> {
    println!("Building all artifacts");
    build_cli()?;
    build_cluster()?;
    build_test()?;
    Ok(())
}

pub fn build_cli() -> Result<()> {
    install_target(None)?;
    println!("Building fluvio");
    cmd!(CARGO, "build", "--bin", "fluvio").run()?;
    Ok(())
}

pub fn build_cluster() -> Result<()> {
    install_target(None)?;
    println!("Building fluvio-run");
    cmd!(CARGO, "build", "--bin", "fluvio-run").run()?;
    Ok(())
}

pub fn build_test() -> Result<()> {
    install_target(None)?;
    println!("Building fluvio-test");
    cmd!(CARGO, "build", "--bin", "fluvio-test").run()?;
    Ok(())
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
