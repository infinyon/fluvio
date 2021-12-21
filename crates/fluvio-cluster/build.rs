use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../VERSION");
    println!("cargo:rerun-if-changed=../../k8-util/helm/pkg_sys/fluvio-chart-sys.tgz");
    println!("cargo:rerun-if-changed=../../k8-util/helm/pkg_app/fluvio-chart-app.tgz");

    let _uname_output = Command::new("make")
        .arg("install")
        .output()
        .expect("package ");
}
