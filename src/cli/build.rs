use std::process::Command;
use rustc_version::version_meta;

fn main() {
    // Fetch current git hash to print version output
    let git_version_output = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .expect("should run 'git rev-parse HEAD' to get git hash");
    let git_hash = String::from_utf8(git_version_output.stdout)
        .expect("should read 'git' stdout to find hash");
    // Assign the git hash to the compile-time GIT_HASH env variable (to use with env!())
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // Fetch OS information if on unix
    if cfg!(unix) {
        let get_uname_options = if cfg!(target_os = "macos") {
            "-srm"
        } else {
            "-srom"
        };
        let options = vec![get_uname_options];
        let uname_output = Command::new("uname")
            .args(&options)
            .output()
            .expect("should get OS info from uname");
        let uname_text =
            String::from_utf8(uname_output.stdout).expect("should read uname output to string");
        println!("cargo:rustc-env=UNAME={}", uname_text);
    }

    // Fetch Rustc information
    let rust_version = version_meta().expect("should get rustc version");
    let semver = &rust_version.semver;
    let rustc_commit = rust_version.commit_hash.as_ref().and_then(|hash| {
        rust_version
            .commit_date
            .as_ref()
            .map(|date| (&hash[..7], date))
    });

    match rustc_commit {
        Some((commit_hash, commit_date)) => {
            println!(
                "cargo:rustc-env=RUSTC_VERSION={} ({} {})",
                semver, commit_hash, commit_date,
            );
        }
        None => {
            println!("cargo:rustc-env=RUSTC_VERSION={}", semver);
        }
    }

}
