use fluvio_connector_derive::connector;

#[connector(source)]
async fn start_fn(config: &[i32], producer: ()) {}
