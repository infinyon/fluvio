#[cfg(feature = "k8")]
#[cfg(not(feature = "k8_stream"))]
mod integratino_tests {

    use lazy_static::lazy_static;
    use log::debug;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::collections::HashMap;

    use future_helper::test_async;
    use k8_client::fixture::TEST_NS;
    use k8_client::ClientError;
    use k8_client::InputK8Obj;
    use k8_client::InputMetadata;
    use k8_client::K8Client;
    use k8_client::ServicePort;
    use k8_client::ServiceSpec;
    use k8_client::ServiceStatus;
    use k8_metadata::core::Spec;

    use types::defaults::SPU_DEFAULT_NAME;

    // way to get static lifetime which is requirmeent for cluster
    lazy_static! {
        static ref K8CLIENT: K8Client = K8Client::new(None).expect("cluster not intialized");
    }

    fn new_service() -> InputK8Obj<ServiceSpec, ServiceStatus> {
        let mut rng = thread_rng();
        let rname: String = rng.sample_iter(&Alphanumeric).take(5).collect();
        let name = format!("test{}", rname);

        let mut labels = HashMap::new();
        labels.insert("app".to_owned(), SPU_DEFAULT_NAME.to_owned());
        let mut selector = HashMap::new();
        selector.insert("app".to_owned(), SPU_DEFAULT_NAME.to_owned());

        let service_spec = ServiceSpec {
            cluster_ip: "None".to_owned(),
            ports: vec![ServicePort {
                port: 9092,
                ..Default::default()
            }],
            selector: Some(selector),
            ..Default::default()
        };

        let new_item: InputK8Obj<ServiceSpec, ServiceStatus> = InputK8Obj {
            api_version: ServiceSpec::api_version(),
            kind: ServiceSpec::kind(),
            metadata: InputMetadata {
                name: name.to_lowercase(),
                labels: Some(labels),
                namespace: TEST_NS.to_string(),
                ..Default::default()
            },
            spec: Some(service_spec),
            status: None,
        };

        new_item
    }

    #[test_async]
    async fn test_client_create_and_delete_service() -> Result<(), ClientError> {
        let new_item = new_service();
        debug!("item: {:#?}", &new_item);
        let item = K8CLIENT.create_item::<ServiceSpec, ServiceStatus>(&new_item).await?;
        debug!("deleting: {:#?}", item);
        K8CLIENT.delete_item::<ServiceSpec>(&new_item.metadata).await?;
        assert!(true, "passed");
        Ok(())
    }

}
