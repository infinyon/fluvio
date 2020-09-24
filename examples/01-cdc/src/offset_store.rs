use async_std::fs;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;

#[derive(Debug)]
pub struct OffsetStore {
    file: PathBuf,
    offset: i64,
}

impl OffsetStore {
    pub async fn init(offset_file: &PathBuf) -> Result<OffsetStore, Error> {
        let file = get_or_create_file(offset_file).await?;
        let offset = read_offset(&file).await?;

        Ok(Self { file, offset })
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub async fn increment_offset(&mut self) -> Result<(), Error> {
        let new_offset = self.offset + 1;
        write_offset(&self.file, new_offset).await?;
        self.offset = new_offset;

        Ok(())
    }
}

async fn get_or_create_file(file_path: &PathBuf) -> Result<PathBuf, Error> {
    match file_path.exists() {
        true => Ok(file_path.clone()),
        false => {
            let parent = file_path.parent().unwrap();
            fs::create_dir_all(&parent).await?;
            fs::write(&file_path, "0").await?;
            Ok(file_path.clone())
        }
    }
}

async fn read_offset(file: &PathBuf) -> Result<i64, Error> {
    let bytes = fs::read(file).await?;
    let data = String::from_utf8(bytes)
        .map_err(|err| Error::new(ErrorKind::InvalidData, format!("{}", err)))?;
    let result = data
        .trim()
        .parse::<i64>()
        .map_err(|err| Error::new(ErrorKind::InvalidData, format!("{}", err)))?;

    Ok(result)
}

async fn write_offset(file: &PathBuf, offset: i64) -> Result<(), Error> {
    fs::write(file, offset.to_string()).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::fs;
    use super::*;

    const TEST_PATH: &str = "test_files";
    const OFFSET_FILE: &str = "offset_file.offset";

    fn build_offset_file_path() -> PathBuf {
        let program_dir = std::env::current_dir().unwrap();
        program_dir.join(TEST_PATH).join(OFFSET_FILE)
    }

    fn cleanup(file: PathBuf) {
        fs::remove_file(file).expect("delete file failed");
    }

    #[test]
    fn test_offset_all() {
        async_std::task::block_on(test_create_file_write_and_read_offset());
        test_increment_offset();
    }

    async fn test_create_file_write_and_read_offset() {
        let offset_path = build_offset_file_path();
        let offset_file = get_or_create_file(&offset_path).await;
        if let Err(err) = &offset_file {
            println!("{:?}", err);
        };
        assert!(offset_file.is_ok());

        let offset_file = offset_file.unwrap();
        let offset = read_offset(&offset_file).await;
        if offset.is_err() {
            println!("{}", offset.as_ref().unwrap_err());
        }
        assert!(offset.is_ok());
        assert_eq!(offset.unwrap(), 0);

        let res = write_offset(&offset_file, 1).await;
        assert!(res.is_ok());

        let offset = read_offset(&offset_file).await;
        assert_eq!(offset.unwrap(), 1);

        cleanup(offset_path);
    }

    fn test_increment_offset() {
        async_std::task::block_on(async {
            let offset_path = build_offset_file_path();
            let mut offset_store = OffsetStore::init(&offset_path).await;
            assert!(offset_store.is_ok());

            let offset_store = offset_store.as_mut().unwrap();
            let res = offset_store.increment_offset().await;
            assert!(res.is_ok());
            assert_eq!(offset_store.offset(), 1);

            let offset_store2 = OffsetStore::init(&offset_path).await;
            assert!(offset_store2.is_ok());
            assert_eq!(offset_store2.unwrap().offset(), 1);

            cleanup(offset_path);
        });
    }
}
