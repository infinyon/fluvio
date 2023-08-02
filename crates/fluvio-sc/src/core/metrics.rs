use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub(crate) struct ScMetrics {}

impl ScMetrics {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
