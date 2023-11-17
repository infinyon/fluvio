use std::process::Command;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let paths = [
        "../../k8-util/helm/pkg_sys/fluvio-chart-sys.tgz",
        "../../k8-util/helm/pkg_app/fluvio-chart-app.tgz",
    ];
    for path in paths {
        // Otherwise when these files are missing, we always rebuild.
        if Path::new(path).is_file() {
            println!("cargo:rerun-if-changed={path}");
        }
    }

    let _ = Command::new("make")
        .arg("install")
        .output()
        .expect("package ");
}
