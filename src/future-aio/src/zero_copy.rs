use std::io::Error as IoError;
use std::fmt;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use nix::sys::sendfile::sendfile;
use nix::Error as NixError;
use async_std::task::spawn_blocking;
use async_std::net::TcpStream;
use async_trait::async_trait;

use crate::fs::AsyncFileSlice;

#[derive(Debug)]
pub enum SendFileError {
    IoError(IoError),
    NixError(NixError),
}

impl fmt::Display for SendFileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::NixError(err) => write!(f, "{}", err),
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

/// zero copy write
#[async_trait]
pub trait ZeroCopyWrite: AsRawFd {
    async fn zero_copy_write(&mut self, source: &AsyncFileSlice) -> Result<usize, SendFileError> {
        let size = source.len();
        let target_fd = self.as_raw_fd();
        let source_fd = source.fd();

        #[cfg(target_os = "linux")]
        let ft = {
            let mut offset = source.position() as i64;
            spawn_blocking(move || {
                sendfile(target_fd, source_fd, Some(&mut offset), size as usize)
                    .map_err(|err| err.into())
            })
        };

        #[cfg(target_os = "macos")]
        let ft = {
            let offset = source.position();
            spawn_blocking(move || {
                log::trace!(
                    "mac zero copy source fd: {} offset: {} len: {}, target: fd{}",
                    source_fd,
                    offset,
                    size,
                    target_fd
                );
                let (res, len) = sendfile(
                    source_fd,
                    target_fd,
                    offset as i64,
                    Some(size as i64),
                    None,
                    None,
                );
                match res {
                    Ok(_) => {
                        log::trace!("mac zero copy bytes transferred: {}", len);
                        Ok(len as usize)
                    }
                    Err(err) => {
                        log::error!("error sendfile: {}", err);
                        Err(err.into())
                    }
                }
            })
        };

        ft.await
    }
}

#[async_trait]
impl ZeroCopyWrite for TcpStream {}

#[cfg(test)]
mod tests {

    use std::net::SocketAddr;
    use std::time;

    use log::debug;

    use futures::stream::StreamExt;
    use async_std::prelude::*;
    use async_std::net::TcpStream;
    use async_std::net::TcpListener;

    use flv_future_core::test_async;
    use futures::future::join;
    use flv_future_core::sleep;

    use crate::fs::file_util;
    use crate::ZeroCopyWrite;
    use crate::fs::AsyncFile;
    use super::SendFileError;

    #[test_async]
    async fn test_zero_copy_from_fs_to_socket() -> Result<(), SendFileError> {
        // spawn tcp client and check contents
        let server = async {
            let listener = TcpListener::bind("127.0.0.1:9999").await?;
            debug!("server: listening");
            let mut incoming = listener.incoming();
            if let Some(stream) = incoming.next().await {
                debug!("server: got connection. waiting");
                let mut tcp_stream = stream?;
                let mut buf = [0; 30];
                let len = tcp_stream.read(&mut buf).await?;
                assert_eq!(len, 30);
            } else {
                assert!(false, "client should connect");
            }
            Ok(()) as Result<(), SendFileError>
        };

        let client = async {
            let file = file_util::open("test-data/apirequest.bin").await?;
            sleep(time::Duration::from_millis(100)).await;
            let addr = "127.0.0.1:9999".parse::<SocketAddr>().expect("parse");
            debug!("client: file loaded");
            let mut stream = TcpStream::connect(&addr).await?;
            debug!("client: connected to server");
            let f_slice = file.as_slice(0, None).await?;
            debug!("client: send back file using zero copy");
            stream.zero_copy_write(&f_slice).await?;
            Ok(()) as Result<(), SendFileError>
        };

        // read file and zero copy to tcp stream

        let _rt = join(client, server).await;
        Ok(())
    }
}
