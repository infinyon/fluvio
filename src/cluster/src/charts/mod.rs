mod sys;

pub use sys::*;

pub use inline::unpack;

const DEFAULT_SYS_NAME: &str = "fluvio-sys";
const DEFAULT_CHART_SYS_NAME: &str = "fluvio/fluvio-sys";
const DEFAULT_CLOUD_NAME: &str = "minikube";

mod inline{
    use std::path::Path;
    use std::fs::{ File, create_dir};
    use std::io::Error as IoError;
    use std::io::Write;

    use tracing::{debug,trace};
    use tempdir::TempDir;
    use include_dir::{include_dir, Dir};

    const SYS_CHART_DIR: Dir = include_dir!("../../k8-util/helm/fluvio-sys");
    const APP_CHART_DIR: Dir = include_dir!("../../k8-util/helm/fluvio-app");


    pub fn unpack() {

        InlineDir::new(SYS_CHART_DIR).expect("unpack");
    }

    pub struct InlineDir(TempDir);
    
    impl InlineDir {

        pub fn new(inline: Dir<'static>) -> Result<Self,IoError> {

            let temp_dir = TempDir::new("sys_chart")?;
            Self::unpack(&inline,temp_dir.path())?;
            Ok(Self(temp_dir))
        }

        // path
        pub fn path(&self) -> &Path {
            self.0.path()
        }

        fn unpack<'a>(inline: &Dir<'a>, base_dir: &Path) -> Result<(),IoError> {

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

            use super::InlineDir;
            use super::SYS_CHART_DIR;

            let inline_chart = InlineDir::new(SYS_CHART_DIR).expect("unpack");
        }

    }

}