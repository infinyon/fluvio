use std::process::Command;

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    // Copy VERSION file. Do not fail e.g. when built via `cargo publish`
    println!("cargo:rerun-if-changed=../../VERSION");

    // Fetch current git hash to print version output
    let git_version_output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("should run 'git rev-parse HEAD' to get git hash");
    let git_hash = String::from_utf8(git_version_output.stdout)
        .expect("should read 'git' stdout to find hash");
    // Assign the git hash to the compile-time GIT_HASH env variable (to use with env!())
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
