
pub use fusable::FusableFuture;

#[cfg(feature = "tokio2")]
pub use sleep_03::sleep;
#[cfg(not(feature = "tokio2"))]
pub use sleep_01::sleep;


#[cfg(feature = "tokio2")]
mod sleep_03 {

    use std::time::Duration;
    use std::time::Instant;

    use tokio_2::timer::Delay;
    use super::FusableFuture;

    pub type FuseableDelay = FusableFuture<Delay>;

    pub fn sleep(duration: Duration) -> FuseableDelay {
        let delay = Delay::new(Instant::now() + duration);
        FuseableDelay::new(delay)
    }
}

#[cfg(not(feature = "tokio2"))]
mod sleep_01 {

    use std::time::Duration;
    use std::time::Instant;

    use tokio_1::timer::Delay;
    use futures::compat::Future01CompatExt;
    use futures::future::FutureExt;
    use futures::future::Future;
    use super::FusableFuture;

    pub fn sleep(duration: Duration) -> impl Future<Output=()>   {
        let delay = Delay::new(Instant::now() + duration).compat();
        FusableFuture::new(delay.map(|_| ()))
    }


}

mod fusable {

    use std::task::Context;
    use std::task::Poll;
    use std::pin::Pin;


    use futures::Future;


    /// add unpin to the arbitrary future which can be potentially unpinned
    pub struct FusableFuture<F> {
        inner: F
    }

    impl <F>Unpin for FusableFuture<F>{}

    impl <F>FusableFuture<F>  {
        pin_utils::unsafe_pinned!(inner: F);
    }

    impl <F>FusableFuture<F> {

        #[allow(unused)]
        pub fn new(inner: F) -> Self {
            Self {
                inner
            }
        }
    }

    impl <F>Future for FusableFuture<F> where F: Future {

        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            
            self.inner().poll(cx)
        }

    }

}
