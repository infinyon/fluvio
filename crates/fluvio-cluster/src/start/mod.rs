pub mod k8;
pub mod local;
mod common;

mod constants {

    use std::env;

    use once_cell::sync::Lazy;

    /// maximum time waiting for network check, DNS or network
    pub static MAX_SC_NETWORK_LOOP: Lazy<u16> = Lazy::new(|| {
        let var_value = env::var("FLV_CLUSTER_MAX_SC_NETWORK_LOOP").unwrap_or_default();
        var_value.parse().unwrap_or(120)
    });
}
