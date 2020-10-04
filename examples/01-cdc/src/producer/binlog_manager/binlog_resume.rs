use std::io::Error;
use std::path::{PathBuf, Path};
use async_std::fs;

use crate::messages::BnFile;

#[derive(Debug, Clone)]
pub struct Resume {
    pub path: PathBuf,
    pub binfile: Option<BnFile>,
}

impl Resume {
    pub fn new<P: Into<PathBuf>>(path: P, binfile: BnFile) -> Result<Self, Error> {
        Ok(Self {
            path: path.into(),
            binfile: Some(binfile),
        })
    }

    pub fn empty<P: Into<PathBuf>>(path: P) -> Result<Self, Error> {
        Ok(Self {
            path: path.into(),
            binfile: None,
        })
    }

    pub async fn load<P: Into<PathBuf>>(resume_path: P) -> Result<Self, Error> {
        let path = resume_path.into();
        let path = match expand_tilde(&path) {
            Some(resolved) => resolved,
            None => path,
        };
        let path = async_std::path::PathBuf::from(path);

        if !path.exists().await {
            let parent = path.parent().unwrap();
            fs::create_dir_all(&parent).await?;
            fs::File::create(&path).await?;
        }
        let bnfile = Self::read_binfile(&path).await?;
        Ok(Self {
            path: path.into(),
            binfile: bnfile,
        })
    }

    pub fn file(&self) -> Option<&str> {
        self.binfile.as_ref().map(|it| &*it.file_name)
    }

    pub fn offset(&self) -> Option<u64> {
        self.binfile.as_ref().and_then(|it| it.offset)
    }

    pub async fn update_binfile(&mut self, binfile: BnFile) -> Result<(), Error> {
        let serialized = serde_json::to_string(&binfile).unwrap();
        println!("Writing binlog: {}", serialized);
        fs::write(&self.path, serialized).await?;
        self.binfile.replace(binfile);
        Ok(())
    }

    async fn read_binfile(path: &async_std::path::Path) -> Result<Option<BnFile>, Error> {
        let resume_contents = fs::read_to_string(&path).await?;
        Ok(serde_json::from_str::<BnFile>(&resume_contents).ok())
    }
}

fn expand_tilde<P: AsRef<Path>>(path_user_input: P) -> Option<PathBuf> {
    let p = path_user_input.as_ref();

    if !p.starts_with("~") {
        return Some(p.to_path_buf());
    }

    if p == Path::new("~") {
        return dirs::home_dir();
    }

    dirs::home_dir().map(|mut h| {
        if h == Path::new("/") {
            p.strip_prefix("~").unwrap().to_path_buf()
        } else {
            h.push(p.strip_prefix("~/").unwrap());
            h
        }
    })
}
