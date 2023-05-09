use fluvio_connector_common::connector;

#[connector(source)]
async fn start_fn(config: CustomConfig, producer: ()) {}

#[connector(config, name = "meta")]
struct CustomConfig {}
