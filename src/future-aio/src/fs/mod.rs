mod async_file;
mod mmap;
mod file_slice;
mod bounded;

pub use self::async_file::AsyncFile;
pub use self::async_file::file_util;
pub use self::file_slice::AsyncFileSlice;
pub use self::bounded::BoundedFileSink;
pub use self::bounded::BoundedFileOption;
pub use self::bounded::BoundedFileSinkError;

pub use self::mmap::MemoryMappedFile;
pub use self::mmap::MemoryMappedMutFile;

// re-export file
pub use async_std::fs::File;
pub use async_std::fs::metadata;


use std::io;
use std::path::Path;
use async_std::fs;

pub async fn create_dir_all<P: AsRef<Path>>(path: P) -> Result<(), io::Error> {

    fs::create_dir_all(path.as_ref()).await
}

