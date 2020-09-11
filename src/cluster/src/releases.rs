use crate::ClusterError;
use crate::helm::HelmClient;

const DEFAULT_CHART_NAME: &str = "fluvio/fluvio-core";

/// List all the available releases
pub fn list_releases() -> Result<(), ClusterError> {
    let helm_client = HelmClient::new()?;
    let versions = helm_client.get_versions(DEFAULT_CHART_NAME)?;
    if versions.len() > 0 {
        println!("VERSION");
        for chart in &versions {
            println!("{}", chart.version);
        }
    } else {
        println!("No releases found");
    }
    Ok(())
}
