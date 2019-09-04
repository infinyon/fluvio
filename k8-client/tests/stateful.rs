#[cfg(feature = "k8")]
#[cfg(not(feature ="k8_stream"))]
mod integratino_tests {

    use lazy_static::lazy_static;

    use k8_client::K8Client;
    use k8_client::StatefulSetSpec;
    use k8_client::StatefulSetStatus;
    use k8_client::ClientError;
    use future_helper::test_async;
    use k8_client::fixture::TEST_NS;

    // way to get static lifetime which is requirement for cluster
    lazy_static! {
        static ref K8CLIENT: K8Client = K8Client::new(None).expect("cluster not intialized");
    }

    // this assume we have at least one statefulset
    #[test_async]
    async fn test_client_get_statefulset() -> Result<(),ClientError> {

        
        K8CLIENT.retrieve_items::<StatefulSetSpec, StatefulSetStatus>(TEST_NS).await?;
        Ok(()) as Result<(),ClientError>        
    }
}
