#[cfg(feature = "k8_stream")]
mod integratino_tests {

    use futures::stream::StreamExt;
    use lazy_static::lazy_static;
    use pin_utils::pin_mut;

    use future_helper::test_async;
    use k8_client::fixture::TEST_NS;
    use k8_client::ClientError;
    use k8_client::K8Client;
    use k8_metadata::topic::TopicSpec;

    // way to get static lifetime which is requirement for cluster
    lazy_static! {
        static ref K8CLIENT: K8Client = K8Client::new(None).expect("cluster not intialized");
    }

    // print first 10 topics of topic stream, this should be only run as part of indiv test
    #[test_async]
    async fn test_client_print_stream() -> Result<(), ClientError> {
        let stream = K8CLIENT.watch_stream_now::<TopicSpec>(TEST_NS.to_owned());
        pin_mut!(stream);
        let mut count = 0;
        let mut end = false;
        while count < 10 && !end {
            match stream.next().await {
                Some(topic) => {
                    count = count + 1;
                    println!("topic: {:#?}", topic);
                }
                _ => {
                    end = true;
                }
            }
        }
        Ok(())
    }

    #[test_async]
    async fn test_client_stream_topics() -> Result<(), ClientError> {
        let stream = K8CLIENT.watch_stream_since::<TopicSpec>(TEST_NS, None);
        pin_mut!(stream);
        let result = stream.next().await;
        match result {
            Some(topic_result) => {
                let topics = topic_result.expect("topics");
                assert!(topics.len() > 0, "there should be at least 1 topic");
            }
            None => {
                assert!(false, "there should be at least 1 topics");
            }
        }

        Ok(())
    }

}
