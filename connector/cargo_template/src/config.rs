use fluvio_connector_common::connector;

#[connector(config)]
#[derive(Debug)]
pub(crate) struct CustomConfig {
    #[allow(dead_code)]
    foo: String,
}
