use proc_macro::TokenStream;

#[proc_macro]
pub fn impl_init_jaeger_macro(_item: TokenStream) -> TokenStream {
    r#"{opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("fluvio_test")
        .install_simple()
        //.install_batch(opentelemetry::runtime::AsyncStd) // deadlock
        .expect("Could not create new tracing pipeline");

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(opentelemetry);

    Some(tracing::subscriber::set_default(subscriber))}"#
        .parse()
        .expect("")
}
