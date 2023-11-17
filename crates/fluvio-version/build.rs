use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    built::write_built_file().expect("Failed to acquire build-time information");

    // Fetch the current git hash and place it in the build environment variables..
    let git_hash = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("`git rev-parse HEAD` failed, are we in a git repository?")
            .stdout,
    )
    .expect("git hash should be a utf-8 string");
    println!("cargo:rustc-env=GIT_HASH={git_hash}");
}
