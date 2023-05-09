use fluvio_connector_derive::connector;

#[connector(source)]
async fn start_fn(self, producer: ()) {}
