pub mod k8;
pub mod local;
mod common;

mod constants {

    use std::env;

    use once_cell::sync::Lazy;

    /// maximum time waiting for SC and SPU to provision
    pub static MAX_PROVISION_TIME_SEC: Lazy<u16> = Lazy::new(|| {
        let var_value = env::var("FLV_CLUSTER_PROVISION_TIMEOUT").unwrap_or_default();
        var_value.parse().unwrap_or(300)
    });
}
