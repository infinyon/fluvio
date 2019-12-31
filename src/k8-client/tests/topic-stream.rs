#[cfg(feature = "k8_stream")]
mod integration_tests {

    use log::debug;
    use futures::stream::StreamExt;
    use flv_future_core::test_async;
    use k8_client::fixture::TEST_NS;
    use k8_client::ClientError;
    use k8_client::K8Client;
    use k8_metadata::topic::TopicSpec;
    use k8_metadata::client::MetadataClient;

    // way to get static lifetime which is requirement for cluster
    fn create_client() -> K8Client {
        K8Client::default().expect("cluster not initialized")
    }

    // print first 10 topics of topic stream, this should be only run as part of indiv test
    #[test_async]
    async fn test_client_print_stream() -> Result<(), ClientError> {
        let client = create_client();
        let mut stream = client.watch_stream_now::<TopicSpec>(TEST_NS.to_owned());
        let mut count: u16 = 0;
        let mut end = false;
        while count < 10 && !end {
            match stream.next().await {
                Some(topic) => {
                    count = count + 1;
                    debug!("topic event: {} {:#?}", count, topic);
                }
                _ => {
                    end = true;
                }
            }
        }
        Ok(())
    }

    /*
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
    */
}
