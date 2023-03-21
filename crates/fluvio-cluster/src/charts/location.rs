use std::path::{PathBuf};

use tracing::{debug, instrument};
use include_dir::{Dir, include_dir};

use fluvio_helm::{HelmClient};

pub use inline::*;

use crate::UserChartLocation;

use super::ChartInstallError;

const SYS_CHART_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/../../k8-util/helm/pkg_sys");
const APP_CHART_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/../../k8-util/helm/pkg_app");

/// Distinguishes between a Local and Remote helm chart
#[derive(Debug, Clone)]
pub enum ChartLocation {
    /// inline chart
    Inline(include_dir::Dir<'static>),
    /// Local charts must be located at a valid filesystem path.
    Local(PathBuf),
    /// Remote charts will be located at a URL such as `https://...`
    Remote(String),
}

impl From<UserChartLocation> for ChartLocation {
    fn from(location: UserChartLocation) -> Self {
        match location {
            UserChartLocation::Local(path) => Self::Local(path),
            UserChartLocation::Remote(location) => Self::Remote(location),
        }
    }
}

impl ChartLocation {
    /// inline chart for app
    pub const fn app_inline() -> Self {
        Self::Inline(APP_CHART_DIR)
    }

    /// inline chart for sys
    pub const fn sys_inline() -> Self {
        Self::Inline(SYS_CHART_DIR)
    }

    /// setup chart to be ready to be installed
    pub fn setup(
        &self,
        name: &str,
        helm_client: &HelmClient,
    ) -> Result<ChartSetup, ChartInstallError> {
        let chart_setup = match &self {
            &ChartLocation::Inline(dir) => {
                debug!("unpacking using inline chart");
                let chart = InlineChart::new(dir)?;
                ChartSetup::Inline(chart)
            }
            ChartLocation::Remote(location) => {
                debug!(
                    %location,
                    %name,
                    "setting up remote helm chart");
                helm_client.repo_add(name, location)?;
                helm_client.repo_update()?;
                ChartSetup::Location(location.to_owned())
            }
            ChartLocation::Local(path) => {
                debug!(chart_location = %path.display(), "Using local helm chart:");
                let chart_location = path.display();
                ChartSetup::Location(chart_location.to_string())
            }
        };

        Ok(chart_setup)
    }

    #[instrument(skip(self))]
    fn setup_remote_chart(&self, chart_location: &str) -> Result<(), ChartInstallError> {
        Ok(())
    }
}

/// Chart that is ready to be install
/// It may be clean up as necessary
pub enum ChartSetup {
    /// inline
    Inline(InlineChart),
    /// location
    Location(String),
}

impl ChartSetup {
    /// location
    pub fn location(&self) -> String {
        match self {
            Self::Inline(inline_chart) => inline_chart.path().display().to_string(),
            Self::Location(location) => location.to_owned(),
        }
    }
}

mod inline {
    use std::path::{Path, PathBuf};
    use std::fs::{DirBuilder, File};
    use std::io::{Error as IoError, ErrorKind};
    use std::io::Write;

    use tracing::{debug, trace};
    use include_dir::{Dir};
    use tempfile::TempDir;

    /// Inline chart contains only a single chart
    pub struct InlineChart {
        _dir: TempDir,
        chart: PathBuf,
    }

    impl InlineChart {
        /// create new inline chart
        pub fn new(inline: &Dir<'static>) -> Result<Self, IoError> {
            let temp_dir = tempfile::Builder::new().prefix("chart").tempdir()?;
            let chart = Self::unpack(inline, temp_dir.path())?;
            Ok(Self {
                _dir: temp_dir,
                chart,
            })
        }

        /// path
        pub fn path(&self) -> &Path {
            &self.chart
        }

        /// find a single chart and return it's physical path
        pub fn unpack(inline: &Dir, base_dir: &Path) -> Result<PathBuf, IoError> {
            debug!(?base_dir, "unpacking inline at base");

            // there should be only 1 chart file in the directory
            if inline.files().count() == 0 {
                return Err(IoError::new(ErrorKind::InvalidData, "no chart found"));
            }
            if inline.files().count() > 1 {
                return Err(IoError::new(
                    ErrorKind::InvalidData,
                    "more than 1 chart file found",
                ));
            }

            let inline_file = inline.files().next().unwrap();
            let chart = base_dir.to_owned().join(inline_file.path());
            trace!(?chart, "writing file");
            let contents = inline_file.contents();
            let mut chart_file = File::create(&chart)?;
            chart_file.write_all(contents)?;
            chart_file.sync_all()?;

            // if there is debug env file, output it as well
            if let Ok(debug_dir) = std::env::var("FLV_INLINE_CHART_DIR") {
                let debug_chart_path = Path::new(&debug_dir);
                let mut builder = DirBuilder::new();
                builder.recursive(true);
                builder
                    .create(debug_chart_path)
                    .expect("FLV_INLINE_CHART_DIR not exists");
                let mut debug_file = File::create(debug_chart_path.join(inline_file.path()))
                    .expect("chart cant' be created");
                debug_file.write_all(contents)?;
                debug_file.sync_all()?;
            }

            Ok(chart)
        }
    }

    #[cfg(test)]
    mod test {

        #[fluvio_future::test]
        async fn test_unpack() {
            use super::InlineChart;
            use super::super::SYS_CHART_DIR;

            let _inline_chart = InlineChart::new(&SYS_CHART_DIR).expect("unpack");
        }
    }
}
