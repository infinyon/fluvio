use std::io;
use std::pin::Pin;
use std::mem::replace;
use std::task::Context;
use std::task::Poll;

use futures::io::AsyncWrite;
use futures::ready;
use futures::future::Future;
use async_std::fs::File;

/// Derived from future io Writeall,
/// Instead of buf restricted to[u8], it supports asref
#[derive(Debug)]
pub struct WriteBufAll<'a, W: ?Sized + 'a + Unpin, B> {
    writer: &'a mut W,
    buf: B,
}

// Pinning is never projected to fields
impl<W: ?Sized + Unpin, B> Unpin for WriteBufAll<'_, W, B> {}

impl<'a, W: AsyncWrite + ?Sized + Unpin, B> WriteBufAll<'a, W, B> {
    pub(super) fn new(writer: &'a mut W, buf: B) -> Self {
        WriteBufAll { writer, buf }
    }
}

impl<W: AsyncWrite + ?Sized + Unpin, B> Future for WriteBufAll<'_, W, B>
where
    B: AsRef<[u8]>,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let mut buf = this.buf.as_ref();
        while !buf.is_empty() {
            let n = ready!(Pin::new(&mut this.writer).poll_write(cx, buf))?;
            {
                let (_, rest) = replace(&mut buf, &[]).split_at(n);
                buf = rest;
            }
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
        }

        Poll::Ready(Ok(()))
    }
}

pub trait AsyncWrite2: AsyncWrite + Unpin {
    fn write_buf_all<'a, B>(&'a mut self, buf: B) -> WriteBufAll<'a, Self, B>
    where
        B: AsRef<[u8]>,
    {
        WriteBufAll::new(self, buf)
    }
}


impl AsyncWrite2 for File{}