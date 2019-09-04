use std::io;
use futures_01::Future;
use futures_01::future::poll_fn;
use tokio_fs::file::File;

#[allow(dead_code)]
pub fn file_clone(mut file: File) -> impl Future<Item=File,Error=io::Error> {
    poll_fn( move || file.poll_try_clone())
}


pub fn file_sync(mut file: File) -> impl Future<Item=(),Error=io::Error> {
     poll_fn( move || file.poll_sync_all())
}


#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use future_helper::Runtime;
    use futures_01::Poll;
    use futures_01::Async;
    use futures_01::Future;
    use log::debug;


    struct TestFuture {
        counter: i32
    }

    impl Future  for  TestFuture {

        type Item = i32;
        type Error = ();

        fn poll(&mut self) -> Poll<i32,()>  {
            
            //let old_count = self.counter.fetch_add(1, Ordering::SeqCst);
            self.counter = self.counter+1;
            debug!("live count: {}", self.counter);

            Ok(Async::Ready(self.counter))
        }
    }
    

    #[test]
    fn test_future() {

        let mut rt = Runtime::new().unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = counter.clone();
        let counter2 = counter.clone();
        let tf2 = TestFuture{ counter: 0};
        let stf2 = tf2.shared();
        let c1 = stf2.clone();       
        rt.spawn(stf2.map( move | count | { 
            debug!("count: {}",*count); 
            counter1.clone().fetch_add(*count as usize, Ordering::SeqCst);
            () 
        } ).map_err(|_| ()));
        rt.spawn(c1.map( move | count | { 
            debug!("second count: {}",*count); 
            counter2.clone().fetch_add(*count as usize, Ordering::SeqCst);
            () 
        } ).map_err(|_| ()));
   
        
        rt.shutdown_on_idle().wait().unwrap();

        let value = Arc::try_unwrap(counter).unwrap();
        assert_eq!(value.into_inner(),2);

    }

}
