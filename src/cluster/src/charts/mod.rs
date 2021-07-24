mod sys;

pub use sys::*;

mod inline{
    use std::path::Path;

    use include_dir::{include_dir, Dir};

    const SYS_CHART_DIR: Dir = include_dir!("../../k8-util/helm/fluvio-sys");
    const APP_CHART_DIR: Dir = include_dir!("../../k8-util/helm/fluvio-app");
}