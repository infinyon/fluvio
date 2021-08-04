use std::process::Command;

fn main() {
    // package helm before build
    println!("cargo:rerun-if-changed=../../k8-util/helm");
    let _uname_output = Command::new("make")
        .arg("install")
        .output()
        .expect("package ");
}
