
use std::io;
use std::io::ErrorKind::Other;
use std::task::Poll;
use std::task::Poll::Ready;
use std::task::Poll::Pending;

use futures::future::poll_fn;

/// Borrowed from Tokio io utils but generalize to return any error

fn blocking_io<F,R,E>(f: F) -> Poll<Result<R,E>>
where
    F: FnOnce() -> Result<R,E>,
    E: From<io::Error>
{
    match tokio_threadpool_2::blocking(f) {
        Ready(Ok(v)) => Ready(v),
        Ready(Err(_)) => Ready(Err(blocking_err().into())),
        Pending => Pending,
    }
}

pub async fn asyncify<F,R,E>(f: F) -> Result<R,E>
where
    F: FnOnce() -> Result<R,E>,
    E: From<io::Error>
{
    let mut f = Some(f);
    poll_fn(move |_| blocking_io(|| f.take().unwrap()())).await
}

fn blocking_err() -> io::Error {
    io::Error::new(
        Other,
        "`blocking` annotated I/O must be called \
         from the context of the Tokio runtime.",
    )
}

