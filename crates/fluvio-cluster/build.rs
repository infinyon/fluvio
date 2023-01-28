use std::process::Command;

fn main() {
    if let Ok(verpath) = std::fs::canonicalize("../../VERSION") {
        if verpath.exists() {
            println!("cargo:rerun-if-changed=../../VERSION");
        }
    }
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../k8-util/helm/pkg_sys/fluvio-chart-sys.tgz");
    println!("cargo:rerun-if-changed=../../k8-util/helm/pkg_app/fluvio-chart-app.tgz");

    let _uname_output = Command::new("make")
        .arg("install")
        .output()
        .expect("package ");

    // Fetch current git hash to print version output
    let git_version_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("should run 'git rev-parse HEAD' to get git hash");
    let git_hash = String::from_utf8(git_version_output.stdout)
        .expect("should read 'git' stdout to find hash");
    // Assign the git hash to the compile-time GIT_HASH env variable (to use with env!())
    //  println!("git hash: {}", git_hash);
    println!("cargo:rustc-env=GIT_HASH={git_hash}");
}
