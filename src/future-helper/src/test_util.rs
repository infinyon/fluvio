/// run async expression and assert based on result value
#[macro_export]
macro_rules! assert_async_block {
    ($ft_exp:expr) => {{
          
        let ft = $ft_exp;
        match flv_future_core::run_block_on(ft)  {
            Ok(_) => log::debug!("finished run"),
            Err(err) => assert!(false,"error {:?}",err)
        }

    }};
}



#[cfg(test)]
mod test {

    use std::io::Error;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    use futures::Future;
    use futures::future::poll_fn;
   

    use crate::test_async;
    
    // fake future helper so that this can be resolved in this crate
    mod flv_future_core {
        pub use crate::run_block_on;
        pub use crate::assert_async_block;
    }
    
    
    // actual test run
    
    #[test_async]
    async fn async_derive_test() -> Result<(),Error> {
        assert!(true,"I am live");
        Ok(())
    }
    
    

    #[test]
    fn test_1_sync_example () {
        
        async fn test_1()  -> Result<(),Error>{
            assert!(true,"works");
            Ok(())
        }

        let ft = async {
            test_1().await
        };

        assert_async_block!(ft);
    }


    struct TestFuture {

    }

    impl Future for TestFuture {

        type Output = u16;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output>  {
            Poll::Ready(2)
        }

    }
   

    #[test_async]
    async fn test_future() -> Result<(),Error> {

        let t = TestFuture{};
        let v: u16 = t.await;
        assert_eq!(v,2);
        Ok(())
    }


    fn test_poll(_cx: &mut Context) -> Poll<u16> {
        Poll::Ready(4)
    }

     #[test_async]
    async fn test_future_with_poll() -> Result<(),Error> {

        assert_eq!(poll_fn(test_poll).await,4);
        Ok(())
    }




}