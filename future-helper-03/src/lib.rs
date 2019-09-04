#![feature(trace_macros)]

#[cfg(feature = "fixture")]
mod test_util;
mod util;

#[cfg(feature = "fixture")]
pub use async_test_derive::test_async;

pub use util::sleep;

#[cfg(feature = "tokio2")]
pub mod tk {
    pub use tokio_2::runtime::Runtime;
    pub use tokio_2::spawn;
    pub use tokio_2::net::TcpStream as TkTcpStream;
    pub use tokio_2::net::TcpListener as TkTcpListner;
}

#[cfg(not(feature = "tokio2"))]
pub mod tk {
    pub use tokio_1::runtime::Runtime;
    pub use tokio_1::spawn;
    pub use tokio_1::net::TcpStream as TkTcpStream;
    pub use tokio_1::net::TcpListener as TkTcpListner;
}

#[cfg(not(feature = "tokio2"))]
use futures_1::Future as Future01;
use futures::future::Future;
use futures::future::FutureExt;
use futures::future::TryFutureExt;
use log::trace;
use log::error;


/// run tokio loop, pass runtime as parameter to closure
/// this differ from tokio run which uses global spawn
pub fn run<F>(spawn_closure: F)
where
    F: Future<Output = ()> + Send + 'static 
{
    #[cfg(feature = "tokio2")] {
        match tk::Runtime::new() {
            Ok(rt) => {
                rt.spawn(spawn_closure);
                rt.shutdown_on_idle();
            },
            Err(err) => error!("can't create runtime: {}",err)
        }
    }

    #[cfg(not(feature = "tokio2"))]
    match tk::Runtime::new() {
        Ok(mut rt) => {
            rt.spawn(futures_1::lazy(|| {
                spawn(spawn_closure);
                Ok(())
            }));
            rt.shutdown_on_idle().wait().unwrap();
        },
        Err(err) => error!("can't create runtime: {}",err)
    }  
}

/// use new future API
pub trait FutureHelper {
    /// block until closure is completed
    fn block_on_ft3<F, R, E>(&mut self, f: F) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        F: Send + 'static + Future<Output = Result<R, E>>;

    fn block_on_all_ft3<F, R, E>(self, f: F) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        F: Send + 'static + Future<Output = Result<R, E>>;


    /// spawn closure
    fn spawn3<F>(&mut self, future: F) -> &mut Self
    where
        F: Future<Output = Result<(), ()>> + Send + 'static;
}


#[cfg(not(feature = "tokio2"))]
pub fn spawn1<F>(future: F)
where
    F: Future<Output = Result<(), ()>> + Send + 'static,
{
    tk::spawn(future.boxed().compat());
}


pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static + Send, 
{
    trace!("spawning future");

    #[cfg(feature = "tokio2")]
    tk::spawn(future);

    #[cfg(not(feature = "tokio2"))]
    spawn1(async {
        future.await;
        Ok(()) as Result<(),()>
    });
     
}


/// create new executor and block until futures is completed
#[cfg(not(feature = "tokio2"))]
pub fn run_block_on<F,R,E>(f:F) -> Result<R,E>
    where
        R: Send + 'static,
        E: Send + 'static,
        F: Send + 'static + Future<Output = Result<R,E>>
{
    tk::Runtime::new().unwrap().block_on(Box::pin(f).compat())  
}


/// run block for i/o bounded futures
/// this is work around tokio runtime issue
 #[cfg(feature = "tokio2")]
pub fn run_block_on<F>(f:F) -> F::Output
    where
        F: Send + 'static + Future,
        F::Output: Send + std::fmt::Debug
{
    let (tx, rx) = tokio_2::sync::oneshot::channel();
    let rt = tokio_2::runtime::Runtime::new().unwrap();
    rt.spawn(async move {
        tx.send(f.await).unwrap();
    });
    rt.block_on(rx).unwrap()
} 



#[cfg(test)]
mod test {

    use lazy_static::lazy_static;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::{thread, time};

    use super::run;
    use super::spawn;


    #[test]
    fn test_spawn3() {
        lazy_static! {
            static ref COUNTER: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
        }

        assert_eq!(*COUNTER.lock().unwrap(), 0);

        let ft = async {
            thread::sleep(time::Duration::from_millis(100));
            *COUNTER.lock().unwrap() = 10;
        };

        run(async {
            spawn(ft);
        });

        assert_eq!(*COUNTER.lock().unwrap(), 10);
    }

}
