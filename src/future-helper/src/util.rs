pub use sleep_03::sleep;



mod sleep_03 {

    use std::time::Duration;

    use futures_timer::Delay;

    pub async fn sleep(duration: Duration)  {
        let delay = Delay::new(duration);
        delay.await;
    }
}


