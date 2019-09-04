mod async_file;
mod sink;
mod mmap;
mod file_slice;

pub use self::async_file::AsyncFile;
pub use self::file_slice::AsyncFileSlice;
pub use self::sink::FileSink;
pub use self::sink::FileSinkError;
pub use self::sink::FileSinkOption;
pub use self::mmap::MemoryMappedFile;
pub use self::mmap::MemoryMappedMutFile;


use std::io;
use std::path::Path;


#[cfg(feature = "tokio2")]
pub async fn create_dir_all<P: AsRef<Path>>(path: P) -> Result<(), io::Error> {

    tokio_2::fs::create_dir_all(path).await
}

#[cfg(not(feature = "tokio2"))]
use futures::Future;

#[cfg(not(feature = "tokio2"))]
pub fn create_dir_all<P: AsRef<Path>>(path: P) -> impl Future<Output = Result<(), io::Error>>  {

    use futures::compat::Future01CompatExt;
    tokio_1::fs::create_dir_all(path).compat()
}
