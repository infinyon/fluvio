use fluvio_connector_derive::connector;

#[connector(source)]
async fn start_fn(config: CustomConfig, producer: ()) {}

#[connector(config, name = "transforms")]
struct CustomConfig {}
