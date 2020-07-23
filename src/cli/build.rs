use std::fs;
use std::process::Command;
use rustc_version::version;

fn main() {

    // Fetch current git hash to print version output
    let git_version_output = Command::new("git").args(&["rev-parse", "HEAD"])
        .output().expect("should run 'git rev-parse HEAD' to get git hash");
    let git_hash = String::from_utf8(git_version_output.stdout)
        .expect("should read 'git' stdout to find hash");
    // Assign the git hash to the compile-time GIT_HASH env variable (to use with env!())
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // Fetch OS information
    let uname_output = Command::new("uname").args(&["-a"]).output().ok();
    let uname_text = uname_output
        .map(|output| String::from_utf8(output.stdout).expect("should read uname output to string"))
        .unwrap_or_else(|| "<this platform does not support uname -a>".to_string());
    println!("cargo:rustc-env=UNAME_ALL={}", uname_text);

    // Fetch Rustc information
    let rust_version = version().expect("should get rustc version");
    println!("cargo:rustc-env=RUSTC_VERSION={}", rust_version);

    println!("cargo:rerun-if-changed=src/VERSION");
    println!("cargo:rerun-if-changed=../../VERSION");
    println!("cargo:rerun-if-changed=build.rs");
    fs::copy("../../VERSION", "src/VERSION").expect("version copy");
}
