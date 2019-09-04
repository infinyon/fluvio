use std::marker::Unpin;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::future::Future;
use futures_1::Async as Async_01;
use std::fmt::Debug;
use std::io;

pub struct BlockIO<F> {
    f: Option<F>,
}

impl<F> Unpin for BlockIO<F> {}

pub fn asyncify<F, R, E>(f: F) -> BlockIO<F>
where
    F: FnOnce() -> Result<R,E>,
{
    BlockIO { f: Some(f) }
}

impl<R,E, F> Future for BlockIO<F>
where
    F: FnOnce() -> Result<R,E>,
    R: Debug,
    E: From<io::Error>
{
    type Output = Result<R,E>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<R,E>> {
        let result = tokio_threadpool_1::blocking(self.f.take().unwrap());
        match result {
            Ok(Async_01::Ready(Ok(v))) => Poll::Ready(Ok(v.into())),
            Ok(Async_01::Ready(Err(err))) => Poll::Ready(Err(err)),
            Ok(Async_01::NotReady) => Poll::Pending,
            Err(_) => Poll::Ready(Err(blocking_err().into())),
        }
    }
}

use std::io::ErrorKind::Other;
pub fn blocking_err() -> io::Error {
    io::Error::new(
        Other,
        "`blocking` annotated I/O must be called \
         from the context of the Tokio runtime.",
    )
}



#[cfg(test)]
mod test {

    use std::io;
    use std::io::Error as IoError;
    use std::{thread, time};
    use future_helper::test_async;

    use super::asyncify;

    #[test_async]
    async fn test_block_io_ok() ->  Result<(), ()> {
      
        let bk = asyncify(|| {
            thread::sleep(time::Duration::from_millis(2000));
            Ok::<u32,IoError>(2)
        });

        let result = bk.await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
        Ok(()) 
    }

    #[test_async]
    async fn test_block_io_err() -> Result<(),()> {
       
        let bk = asyncify(|| {
            thread::sleep(time::Duration::from_millis(100));
            let result: io::Result<()> = Result::Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server has terminated connection",
            ));
            result
        });

        let result = bk.await;
        assert!(result.is_err());
        Ok(()) 
    }
}