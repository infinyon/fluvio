use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Write;
use std::io::Read;
use std::fs::File as SyncFile;
use std::fs::Metadata as SyncMetadata;
use std::fs::metadata as sync_metadata_fn;
use std::io::SeekFrom;
use std::io::Seek;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Poll;
use std::task::Context;


#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;

use log::trace;
use futures::io::AsyncWrite;
use futures::io::AsyncRead;
use futures::Future;
use pin_utils::pin_mut;


use crate::asyncify;
use crate::AsyncWrite2;
use super::AsyncFileSlice;


#[derive(Debug)]
pub struct AsyncFile{
    file: SyncFile,
    path: PathBuf
}


impl Drop for AsyncFile {
    fn drop(&mut self) {
        trace!("dropping file: {:#?}",self.path);
    }
}



impl std::fmt::Display for AsyncFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

       write!(f, "path = {:#?},fd = {:#?}", self.path,self.file.as_raw_fd())
    }
}

impl AsyncFile  {

    fn new<P>(file: SyncFile,path: P) -> Self  where P: AsRef<Path>{
        AsyncFile {
            file,
            path: path.as_ref().to_path_buf()
        }
    }

    /// open for write only
    pub async fn create<P>(path: P) -> Result<Self,IoError> where P: AsRef<Path>
     {
        let file_path = path.as_ref();
        asyncify( move || SyncFile::create(file_path)).await.map(|sf| AsyncFile::new(sf,file_path))

    }

    /// open for only read
    pub async fn open<P>(path: P) -> Result<Self,IoError> 
        where P: AsRef<Path>
    {
        let file_path = path.as_ref();
        let sf = asyncify( || SyncFile::open(file_path)).await?;
        Ok(AsyncFile::new(sf,file_path))
    }

    /// open for read and write
    pub async fn open_read_write<P>(path: P) -> Result<Self,IoError> 
        where P: AsRef<Path>
    {
        let file_path = path.as_ref();
        let mut option = std::fs::OpenOptions::new();
        option.read(true)
            .write(true)
            .create(true)
            .append(false);

        let sf = asyncify( || option.open(file_path)).await?;
        Ok(AsyncFile::new(sf,file_path))
    }

    pub async fn open_read_append<P>(path: P) -> Result<Self,IoError> 
        where P: AsRef<Path>
    {
        let file_path = path.as_ref();
        let mut option = std::fs::OpenOptions::new();
        option.read(true)
            .create(true)
            .append(true);

        let sf = asyncify( || option.open(file_path)).await?;
        Ok(AsyncFile::new(sf,file_path))
    }


    pub fn get_metadata<P>(path: P) -> impl Future<Output=Result<SyncMetadata,IoError>>
        where P: AsRef<Path>
    {
        asyncify( move || sync_metadata_fn(path))
    }


    pub fn from_std(file: SyncFile,path: PathBuf) -> Self {
        AsyncFile::new(file,path)
    }

    pub fn get_path(&self) -> &Path {
        self.path.as_path()
    }

    
    pub fn set_len(&mut self,size: u64) -> impl Future<Output=Result<(),IoError>> + '_{
        asyncify( move || self.file.set_len(size))
    }

    pub fn sync_all(&mut self) -> impl Future<Output=Result<(),IoError>> + '_ {
        asyncify( move || self.file.sync_all())
    }

    pub fn metadata(&self) -> impl Future<Output=Result<SyncMetadata,IoError>> + '_  {
        asyncify( move || self.file.metadata())
    }

    pub fn write<'a>(&'a mut self, buf: &'a [u8]) -> impl Future<Output=Result<usize,IoError>> + 'a {
        asyncify( move || self.file.write(buf))
    }


    pub fn read<'a>(&'a mut self,buf: &'a mut [u8]) -> impl Future<Output=Result<usize,IoError>> + 'a {
        asyncify( move || self.file.read(buf))
    }

    pub fn seek(&mut self, pos: SeekFrom) -> impl Future<Output=Result<u64,IoError>> + '_ {
        asyncify( move || self.file.seek(pos))
    }



    pub async fn try_clone(&self) -> Result<Self,IoError> {
        let sf = asyncify(|| self.file.try_clone()).await?;
        Ok(AsyncFile::from_std(sf,self.path.clone()))
    }


    // create open
    pub async fn read_clone(&self) -> Result<Self,IoError> {
        trace!("creatinhg clone for path: {:#?}",self.path);
        Self::open(&self.path).await
    }

    pub async fn reset_to_beggining(&mut self) -> Result<(),IoError> {
        self.seek(SeekFrom::Start(0)).await.map(|_| ())
    }

    /// return raw slice with fiel descriptor, this doesn't not check
    pub fn raw_slice(&self, position: u64, len: u64) -> AsyncFileSlice {
        AsyncFileSlice::new(
            self.as_raw_fd(),
            position,
            len
        )
    }

    /// Extract slice of file using file descriptor
    pub async fn as_slice(&self, position: u64,desired_len_opt: Option<u64> ) -> Result<AsyncFileSlice,IoError> {
        
        let metadata = self.metadata().await?;
        let len = metadata.len();
        
        if position >= len {
            return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "position is greater than available len",
            ));
        }
        let slice_len = if let Some(desired_len) = desired_len_opt {
            if position + desired_len >= len {
                return Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "not available bytes",
                ));
            }
            desired_len
        } else {
            len - position
        };

        trace!("file trace: position: {}, len: {}", position,len);
        
        Ok(self.raw_slice(position,slice_len))
    }

}


impl AsyncWrite for AsyncFile {
    
    fn poll_write(mut self: Pin<&mut Self>, ctx: &mut Context, buf: &[u8]) -> Poll<Result<usize,IoError>> {
        trace!("writing: {} bytes",buf.len());
        let ft = self.write(buf);
        pin_mut!(ft);
        ft.poll(ctx)
    }
    
    
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(),IoError>> {
        let ft = self.sync_all();
        pin_mut!(ft);
        ft.poll(cx)
    }
    

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), IoError>> {
        Poll::Ready(Ok(()))
    }
    
}


impl AsyncWrite2 for AsyncFile {}


impl AsyncRead for AsyncFile {

    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize,IoError>> {
        trace!("reading  bytes");
        let ft = self.read(buf);
        pin_mut!(ft);
        ft.poll(cx)
    }

}


impl AsRawFd for AsyncFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}




#[cfg(test)]
mod tests {


   
    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Error as IoError;
    use std::io::Write;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Read;
   
    use futures::io::AsyncReadExt;
    use futures::io::AsyncWriteExt;
   
    use future_helper::test_async;
    use utils::fixture::ensure_clean_file;
    use super::AsyncFile;


    // sync seek write and read
    // this is used for implementating async version
     #[test]
    fn test_sync_seek_write() -> Result<(),std::io::Error> {
        
        let mut option = std::fs::OpenOptions::new();
        option.read(true)
            .write(true)
            .create(true)
            .append(false);
        
        let mut file = option.open("/tmp/x1")?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(b"test")?;
      //  file.write_all(b"kkk")?;
        file.sync_all()?;

        let mut f2 = File::open("/tmp/x1")?;
        let mut contents = String::new();
        f2.read_to_string(&mut contents)?;
        assert_eq!(contents,"test");
        Ok(())
   
    }



    #[test_async]
    async fn async_file_write_read_multiple() -> Result<(), IoError> {
       
        let test_file_path = temp_dir().join("file_write_test");
        ensure_clean_file(&test_file_path);

        
        let mut file = AsyncFile::create(&test_file_path).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(b"test").await?;

        let mut f2 = AsyncFile::create(&test_file_path).await?;
        f2.seek(SeekFrom::Start(0)).await?;
        f2.write_all(b"xyzt").await?;

        let mut output = Vec::new();
        let mut rfile = AsyncFile::open(&test_file_path).await?;
        rfile.read_to_end(&mut output).await?;
        assert_eq!(output.len(),4);
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents,"xyzt");
      
        Ok(()) 
        
    }


    #[test_async]
    async fn async_file_write_read_same() -> Result<(), IoError> {
       
        let test_file_path = temp_dir().join("read_write_test");
        ensure_clean_file(&test_file_path);

        let mut output = Vec::new();
        let mut file = AsyncFile::open_read_write(&test_file_path).await?;
        file.write_all(b"test").await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_end(&mut output).await?;
        assert_eq!(output.len(),4);
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents,"test");
      
        Ok(()) 
        
    }

    #[test_async]
    async fn async_file_write_append_same() -> Result<(), IoError> {
       
        let test_file_path = temp_dir().join("read_append_test");
        ensure_clean_file(&test_file_path);

        let mut output = Vec::new();
        let mut file = AsyncFile::open_read_append(&test_file_path).await?;
        file.write_all(b"test").await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(b"xyz").await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.read_to_end(&mut output).await?;
        assert_eq!(output.len(),7);
        let contents = String::from_utf8(output).expect("conversion");
        assert_eq!(contents,"testxyz");
      
        Ok(()) 
        
    }




   


}

