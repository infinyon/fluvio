
use std::io::Error as IoError;
use std::fmt;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use std::pin::Pin;
use std::task::Context;

use futures::Future;
use futures::Poll;
use pin_utils::pin_mut;
use nix::sys::sendfile::sendfile;
use nix::Error as NixError;

use crate::fs::AsyncFileSlice;
use crate::asyncify;

/// zero copy write
pub trait ZeroCopyWrite  {

    
    fn zero_copy_write<'a>(&'a mut self, source: &'a AsyncFileSlice) -> ZeroCopyFuture<'a,Self> {
        ZeroCopyFuture {
            source,
            writer: self
        }
    }
    
}



#[derive(Debug)]
pub enum SendFileError {
    IoError(IoError),
    NixError(NixError)
}


impl fmt::Display for SendFileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::NixError(err) => write!(f,"{}",err),
        }
    }
}



impl From<IoError> for SendFileError {
    fn from(error: IoError) -> Self {
        SendFileError::IoError(error)
    }
}

impl From<NixError> for SendFileError {
    fn from(error: NixError) -> Self {
        SendFileError::NixError(error)
    }
}

/// similar to Writeall
#[allow(dead_code)]
pub struct ZeroCopyFuture<'a,W: ?Sized + 'a>  {
    writer: &'a mut W,
    source: &'a AsyncFileSlice,
}

impl <'a,W>ZeroCopyFuture<'a,W>  {

    #[allow(dead_code)]
    pub fn new(writer: &'a mut W, source: &'a AsyncFileSlice) -> ZeroCopyFuture<'a,W> {
        ZeroCopyFuture {
            writer,
            source
        }
    }
}

impl <W>Future for ZeroCopyFuture<'_,W> where W: ?Sized + AsRawFd {
    
    type Output = Result<usize,SendFileError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<usize,SendFileError>> {

        let size = self.source.len();
        let target_fd = self.writer.as_raw_fd();
        let source_fd = self.source.as_raw_fd();

        #[cfg(target_os="linux")]
        let ft = asyncify( move || {
            let mut offset = self.source.position() as i64;
            sendfile(target_fd,source_fd,Some(&mut offset),size as usize).map_err(|err| err.into())
        });


        #[cfg(target_os="macos")]
        let ft = asyncify( move || { 
            let offset = self.source.position();
            log::trace!("mac zero copy source fd: {} offset: {} len: {}, target: fd{}",source_fd,offset,size,target_fd);
            let (res,len) = sendfile(source_fd,target_fd,offset as i64,Some(size as i64),None,None);
            match res {
                Ok(_) => {
                    log::trace!("mac zero copy bytes transferred: {}",len);
                    Ok(len as usize)
                },
                Err(err) => Err(err.into())
            }
        });

        pin_mut!(ft);
        ft.poll(cx)

    }
    
}


#[cfg(test)]
mod tests {

    use std::net::TcpListener;
    use std::net::SocketAddr;
    use std::io::Error;
    use std::thread;
    use std::time;
    use std::io::Read;


    use log::debug;
    use future_helper::test_async;

    use crate::fs::AsyncFile;
    use crate::net::AsyncTcpStream;
    use super::ZeroCopyWrite;
    use super::SendFileError;
    

    #[test_async]
    async fn test_copy() -> Result<(),SendFileError> {

        let handle = thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:9999")?;
           
           for st_res in listener.incoming() {
               let mut stream = st_res?;
                debug!("server: got connection. waiting");
                let mut buf = [0; 30];
                let len = stream.read(&mut buf)?;
                assert_eq!(len,30);
                return Ok(()) as Result<(),Error>
                
           } 
           Ok(()) as Result<(),Error>
        });

        // test data 
        let file = AsyncFile::open("test-data/apirequest.bin").await?;
        thread::sleep(time::Duration::from_millis(100)); // give time for server to come up
        let addr = "127.0.0.1:9999".parse::<SocketAddr>().expect("parse");

        let mut stream = AsyncTcpStream::connect(&addr).await?;

        let fslice = file.as_slice(0,None).await?;
        stream.zero_copy_write(&fslice).await?;
        match handle.join() {
            Err(_) => assert!(false,"thread not finished"),
            _ => ()
        }
    
        Ok(())
    }

}




