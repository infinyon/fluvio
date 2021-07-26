
use std::path::{PathBuf};


use tracing::{debug, instrument};
use include_dir::{ Dir,include_dir};

use fluvio_helm::{HelmClient};

pub use inline::*;

use super::ChartInstallError;

const SYS_CHART_DIR: Dir = include_dir!("../../k8-util/helm/fluvio-sys");
const APP_CHART_DIR: Dir = include_dir!("../../k8-util/helm/fluvio-app");


/// Distinguishes between a Local and Remote helm chart
#[derive(Debug, Clone)]
pub enum ChartLocation {
    Inline(include_dir::Dir<'static>),
    /// Local charts must be located at a valid filesystem path.
    Local(PathBuf),
    /// Remote charts will be located at a URL such as `https://...`
    Remote(String),
}

impl ChartLocation {

    pub const fn app_inline() -> Self {
        Self::Inline(APP_CHART_DIR)
    }

    pub const fn sys_inline() -> Self {
        Self::Inline(SYS_CHART_DIR)
    }

    /// setup chart to be ready to be installed
    pub fn setup(&self,name: &str, helm_client: &HelmClient) -> Result<ChartSetup,ChartInstallError> {

        let chart_setup = match self {
            &ChartLocation::Inline(dir) => {
               
                let chart = InlineChart::new(dir)?;
                ChartSetup::Inline(chart)
            },
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
    fn setup_remote_chart(
        &self,
        chart_location: &str,
    ) -> Result<(), ChartInstallError> {
        
        Ok(())
    }

}



pub enum ChartSetup {
    Inline(InlineChart),
    Location(String)
}

impl ChartSetup {

    pub fn location(&self) -> String {
        match self {
            Self::Inline(inline_chart) => inline_chart.path().display().to_string(),
            Self::Location(location) => location.to_owned()
        }
    }
}



mod inline{
    use std::path::Path;
    use std::fs::{ File, create_dir};
    use std::io::Error as IoError;
    use std::io::Write;

    use tracing::{debug,trace};
    use tempdir::TempDir;
    use include_dir::{Dir};



    pub struct InlineChart(TempDir);
    
    impl InlineChart {

        pub fn new(inline: Dir<'static>) -> Result<Self,IoError> {

            let temp_dir = TempDir::new("sys_chart")?;
            Self::unpack(&inline,temp_dir.path())?;
            Ok(Self(temp_dir))
        }

        // path
        pub fn path(&self) -> &Path {
            self.0.path()
        }

        pub fn unpack<'a>(inline: &Dir<'a>, base_dir: &Path) -> Result<(),IoError> {

            debug!(?base_dir,"unpacking inline at base");

            for inline_file in inline.files {
                let sub_file = base_dir.to_owned().join(inline_file.path());
                trace!(?sub_file,"writing file");
                let contents = inline_file.contents();
                let mut file = File::create(sub_file)?;
                file.write_all(contents)?;
            }

            for dir in inline.dirs() {
                let sub_dir = base_dir.to_owned().join(dir.path());
                trace!(?sub_dir,"creating sub dir");
                create_dir(&sub_dir)?;
                Self::unpack(dir,base_dir)?;
            }
    
            Ok(())
    
        }

    }


    #[cfg(test)]
    mod test {

    
        #[fluvio_future::test]
        async fn test_unpack() {

            use super::InlineChart;
            use super::super::SYS_CHART_DIR;

            let inline_chart = InlineChart::new(SYS_CHART_DIR).expect("unpack");
        }

    }

}