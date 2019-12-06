use std::pin::Pin;
use std::marker::Unpin;
use std::task::Context;

use futures_1::executor::{
    spawn as spawn01, Notify as Notify01, NotifyHandle as NotifyHandle01,
    Spawn as Spawn01, UnsafeNotify as UnsafeNotify01,
};
use futures_1::{
    Async as Async01, AsyncSink as AsyncSink01,
    Sink as Sink01, Stream as Stream01,
};
use futures::{task as task03, Stream as Stream03};
use futures::sink::Sink as Sink03;


struct NotifyWaker(task03::Waker);

// from futures source
#[derive(Clone)]
struct WakerToHandle<'a>(&'a task03::Waker);

impl<'a> From<WakerToHandle<'a>> for NotifyHandle01 {
    fn from(handle: WakerToHandle<'a>) -> NotifyHandle01 {
        let ptr = Box::new(NotifyWaker(handle.0.clone()));

        unsafe { NotifyHandle01::new(Box::into_raw(ptr)) }
    }
}

impl Notify01 for NotifyWaker {
    fn notify(&self, _: usize) {
        self.0.wake_by_ref();
    }
}

unsafe impl UnsafeNotify01 for NotifyWaker {
    unsafe fn clone_raw(&self) -> NotifyHandle01 {
        WakerToHandle(&self.0).into()
    }

    unsafe fn drop_raw(&self) {
        let ptr: *const dyn UnsafeNotify01 = self;
        drop(Box::from_raw(ptr as *mut dyn UnsafeNotify01));
    }
}



#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Compat01As03Sink<S, SinkItem> {
    pub(crate) inner: Spawn01<S>,
    pub(crate) buffer: Option<SinkItem>,
    pub(crate) close_started: bool,
}

impl<S, SinkItem> Unpin for Compat01As03Sink<S, SinkItem> {}

impl<S, SinkItem> Compat01As03Sink<S, SinkItem> {
    /// Wraps a futures 0.1 Sink object in a futures 0.3-compatible wrapper.
    pub fn new(inner: S) -> Compat01As03Sink<S, SinkItem> {
        Compat01As03Sink {
            inner: spawn01(inner),
            buffer: None,
            close_started: false
        }
    }

    fn in_notify<R>(
        &mut self,
        cx: &mut Context,
        f: impl FnOnce(&mut S) -> R,
    ) -> R {
        let notify = &WakerToHandle(cx.waker());
        self.inner.poll_fn_notify(notify, 0, f)
    }

    #[allow(dead_code)]
    pub(crate) fn get_inner(&self) -> &S {
        self.inner.get_ref()
    }


}

impl<S, SinkItem> Stream03 for Compat01As03Sink<S, SinkItem>
where
    S: Stream01,
{
    type Item = Result<S::Item, S::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> task03::Poll<Option<Self::Item>> {
        match self.in_notify(cx, |f| f.poll()) {
            Ok(Async01::Ready(Some(t))) => task03::Poll::Ready(Some(Ok(t))),
            Ok(Async01::Ready(None)) => task03::Poll::Ready(None),
            Ok(Async01::NotReady) => task03::Poll::Pending,
            Err(e) => task03::Poll::Ready(Some(Err(e))),
        }
    }
}

impl<S, SinkItem> Sink03<SinkItem> for Compat01As03Sink<S, SinkItem>
where
    S: Sink01<SinkItem = SinkItem>,
{
    type Error = S::SinkError;

    fn start_send(
        mut self: Pin<&mut Self>,
        item: SinkItem
    ) -> Result<(), Self::Error> {
        debug_assert!(self.buffer.is_none());
        self.buffer = Some(item);
        Ok(())
    }

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> task03::Poll<Result<(), Self::Error>> {
        match self.buffer.take() {
            Some(item) => match self.in_notify(cx, |f| f.start_send(item)) {
                Ok(AsyncSink01::Ready) => task03::Poll::Ready(Ok(())),
                Ok(AsyncSink01::NotReady(i)) => {
                    self.buffer = Some(i);
                    task03::Poll::Pending
                }
                Err(e) => task03::Poll::Ready(Err(e)),
            },
            None => task03::Poll::Ready(Ok(())),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> task03::Poll<Result<(), Self::Error>> {
        let item = self.buffer.take();
        match self.in_notify(cx, |f| match item {
            Some(i) => match f.start_send(i) {
                Ok(AsyncSink01::Ready) => f.poll_complete().map(|i| (i, None)),
                Ok(AsyncSink01::NotReady(t)) => {
                    Ok((Async01::NotReady, Some(t)))
                }
                Err(e) => Err(e),
            },
            None => f.poll_complete().map(|i| (i, None)),
        }) {
            Ok((Async01::Ready(_), _)) => task03::Poll::Ready(Ok(())),
            Ok((Async01::NotReady, item)) => {
                self.buffer = item;
                task03::Poll::Pending
            }
            Err(e) => task03::Poll::Ready(Err(e)),
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> task03::Poll<Result<(), Self::Error>> {
        let item = self.buffer.take();
        let close_started = self.close_started;

        match self.in_notify(cx, |f| match item {
            Some(i) => match f.start_send(i) {
                Ok(AsyncSink01::Ready) => {
                    match f.poll_complete() {
                        Ok(Async01::Ready(_)) => {
                            match <S as Sink01>::close(f) {
                                Ok(i) => Ok((i, None, true)),
                                Err(e) => Err(e)
                            }
                        },
                        Ok(Async01::NotReady) => Ok((Async01::NotReady, None, false)),
                        Err(e) => Err(e)
                    }
                },
                Ok(AsyncSink01::NotReady(t)) => {
                    Ok((Async01::NotReady, Some(t), close_started))
                }
                Err(e) => Err(e),
            },
            None => if close_started {
                match <S as Sink01>::close(f) {
                    Ok(i) => Ok((i, None, true)),
                    Err(e) => Err(e)
                }
            } else {
                match f.poll_complete() {
                    Ok(Async01::Ready(_)) => {
                        match <S as Sink01>::close(f) {
                            Ok(i) => Ok((i, None, true)),
                            Err(e) => Err(e)
                        }
                    },
                    Ok(Async01::NotReady) => Ok((Async01::NotReady, None, close_started)),
                    Err(e) => Err(e)
                }
            },
        }) {
            Ok((Async01::Ready(_), _, _)) => task03::Poll::Ready(Ok(())),
            Ok((Async01::NotReady, item, close_started)) => {
                self.buffer = item;
                self.close_started = close_started;
                task03::Poll::Pending
            }
            Err(e) => task03::Poll::Ready(Err(e)),
        }
    }
}