#[cfg(feature = "telemetry")]
pub use fluvio_test_init_tracing_macro::impl_init_jaeger_macro as init_jaeger;

#[cfg(not(feature = "telemetry"))]
#[macro_export]
macro_rules! init_jaeger {
    () => {
        None
    };
}

#[cfg(not(feature = "telemetry"))]
pub use init_jaeger;
