#[cfg(feature = "k8")]
#[cfg(not(feature = "k8_stream"))]
mod integration_tests {

    use std::collections::HashMap;

    use log::debug;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use flv_future_core::test_async;
    use k8_client::fixture::TEST_NS;
    use k8_client::ClientError;
    use k8_metadata::core::metadata::InputK8Obj;
    use k8_metadata::core::metadata::InputObjectMeta;
    use k8_client::K8Client;
    use k8_client::service::ServicePort;
    use k8_client::service::ServiceSpec;
    use k8_metadata::core::Spec;
    use k8_metadata::client::MetadataClient;
    use types::defaults::SPU_DEFAULT_NAME;

    fn create_client() -> K8Client {
        K8Client::default().expect("cluster not initialized")
    }

    fn new_service() -> InputK8Obj<ServiceSpec> {
        let rng = thread_rng();
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

        let new_item: InputK8Obj<ServiceSpec> = InputK8Obj {
            api_version: ServiceSpec::api_version(),
            kind: ServiceSpec::kind(),
            metadata: InputObjectMeta {
                name: name.to_lowercase(),
                labels,
                namespace: TEST_NS.to_string(),
                ..Default::default()
            },
            spec: service_spec,
            ..Default::default()
        };

        new_item
    }

    #[test_async]
    async fn test_client_create_and_delete_service() -> Result<(), ClientError> {
        let new_item = new_service();
        debug!("item: {:#?}", &new_item);
        let client = create_client();
        let item = client.create_item::<ServiceSpec>(new_item).await?;
        debug!("deleting: {:#?}", item);
        let input_metadata: InputObjectMeta = item.metadata.into();
        client
            .delete_item::<ServiceSpec, _>(&input_metadata)
            .await?;
        assert!(true, "passed");
        Ok(())
    }
}
