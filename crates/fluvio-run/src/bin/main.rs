use clap::Parser;
use fluvio_run::RunCmd;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd: RunCmd = RunCmd::parse();

    #[cfg(feature = "telemetry")]
    let _jaeger_guard = {
        use tracing::Level;
        use tracing_subscriber::{Registry, prelude::*};
        use tracing_subscriber::filter::Directive;

        let service_name = match cmd {
            RunCmd::SC(_) => "fluvio-sc",
            RunCmd::SPU(_) => "fluvio-spu",
            _ => "fluvio-run",
        };

        let (tracer, guard) = opentelemetry_jaeger::new_pipeline()
            .with_service_name(service_name)
            .install()
            .unwrap();

        Registry::default()
            .with(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(Directive::from(Level::DEBUG)),
            )
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();

        guard
    };

    #[cfg(not(feature = "telemetry"))]
    fluvio_future::subscriber::init_tracer(None);

    cmd.process()?;
    Ok(())
}
