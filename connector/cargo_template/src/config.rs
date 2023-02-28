
use fluvio_connector_common::connector;

#[connector(config, name = "{{project-name}}")]
#[derive(Debug)]
pub(crate) struct {{project-name}}Config {
}
