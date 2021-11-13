use std::process::Command;

fn main() {
    // force it re-run everytime
    println!("cargo:rerun-if-changed=../../k8-util/helm/pkg_app/fluvio-app-none.tgz");
    let _uname_output = Command::new("make")
        .arg("install")
        .output()
        .expect("package ");
}
