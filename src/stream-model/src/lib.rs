pub mod core;
pub mod store;

#[cfg(feature = "k8")]
pub mod k8 {

    pub mod core {
        pub use k8_obj_core::*;
    }

    pub mod app {
        pub use k8_obj_app::*;
    }

    pub mod metadata {
        pub use k8_obj_metadata::*;
    }
}
