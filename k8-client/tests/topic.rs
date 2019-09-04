#[cfg(feature = "k8")]
#[cfg(not(feature = "k8_stream"))]
mod integratino_tests {

    use lazy_static::lazy_static;
    use log::debug;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use serde_json;
    use serde_json::json;
    use std::collections::BTreeMap;

    use future_helper::test_async;
    use k8_client::fixture::TEST_NS;
    use k8_client::ApplyResult;
    use k8_client::ClientError;
    use k8_client::InputK8Obj;
    use k8_client::InputMetadata;
    use k8_client::K8Client;
    use k8_client::K8Obj;
    use k8_client::{PodSpec, PodStatus};
    use k8_metadata::cluster::{ClusterSpec, ClusterStatus};
    use k8_metadata::core::Spec;
    use k8_metadata::topic::{TopicSpec, TopicStatus, TopicStatusResolution};

    // way to get static lifetime which is requirement for cluster
    lazy_static! {
        static ref K8CLIENT: K8Client = K8Client::new(None).expect("cluster not intialized");
    }

    fn new_topic() -> InputK8Obj<TopicSpec, TopicStatus> {
        let mut rng = thread_rng();
        let rname: String = rng.sample_iter(&Alphanumeric).take(5).collect();
        let name = format!("test{}", rname);
        debug!("create topic with name: {}", &name);
        let topic_spec = TopicSpec {
            partitions: 2,
            replication_factor: 5,
            ignore_rack_assignment: Some(true),
        };

        let new_item: InputK8Obj<TopicSpec, TopicStatus> = InputK8Obj {
            api_version: TopicSpec::api_version(),
            kind: TopicSpec::kind(),
            metadata: InputMetadata {
                name: name.to_lowercase(),
                namespace: TEST_NS.to_string(),
                ..Default::default()
            },
            spec: Some(topic_spec),
            status: None,
        };

        new_item
    }

    #[test_async]
    async fn test_client_get_topic() -> Result<(), ClientError> {
        let topic: K8Obj<TopicSpec, TopicStatus> =
            K8CLIENT.retrieve_item(InputMetadata::named("test", TEST_NS)).await?;
        assert_eq!(topic.kind.unwrap(), "Topic");
        assert_eq!(topic.metadata.name.unwrap(), "test");
        assert_eq!(topic.spec.as_ref().unwrap().partitions, 1);

        Ok(())
    }

    #[test_async]
    async fn test_client_get_topics() -> Result<(), ClientError> {
        let topics = K8CLIENT.retrieve_items::<TopicSpec, TopicStatus>(TEST_NS).await?;
        assert!(topics.items.len() > 0);
        assert_eq!(topics.kind, "TopicList");
        let topic = &topics.items[0];
        assert_eq!(topic.kind.as_ref().unwrap(), "Topic");
        assert_eq!(topic.metadata.name, Some("test".to_owned()));
        assert_eq!(topic.spec.as_ref().unwrap().partitions, 1);
        Ok(())
    }

    #[test_async]
    async fn test_client_create_and_delete_topic() -> Result<(), ClientError> {
        let new_item = new_topic();
        K8CLIENT.create_item::<TopicSpec, TopicStatus>(&new_item).await?;
        K8CLIENT.delete_item::<TopicSpec>(&new_item.metadata).await?;
        Ok(())
    }

    #[test_async]
    async fn test_client_create_update_topic_status() -> Result<(), ClientError> {
        let new_item = new_topic();
        let mut topic = K8CLIENT.create_item::<TopicSpec, TopicStatus>(&new_item).await?;

        // assign topic status
        let mut par: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
        par.insert(0, vec![0, 1]);
        let status = TopicStatus {
            resolution: TopicStatusResolution::Ok,
            replica_map: Some(par),
            target_replica_map: None,
            reason: None,
        };
        topic.status = Some(status);

        let topic = K8CLIENT.update_status(&topic).await?;
        let status = topic.status.unwrap();
        assert_eq!(status.replica_map.unwrap().get(&0).unwrap().len(), 2);

        K8CLIENT.delete_item::<TopicSpec>(&new_item.metadata).await?;

        Ok(())
    }

    #[test_async]
    async fn test_client_get_pods() -> Result<(), ClientError> {
        K8CLIENT.retrieve_items::<PodSpec, PodStatus>(TEST_NS).await?;
        Ok(())
    }

    #[test_async]
    async fn test_client_get_clusters() -> Result<(), ClientError> {
        let clusters = K8CLIENT.retrieve_items::<ClusterSpec, ClusterStatus>(TEST_NS).await?;
        assert!(
            clusters.items.len() > 0,
            "at least 1 cluster should be there"
        );
        Ok(())
    }

    #[test_async]
    async fn test_client_patch_topic() -> Result<(), ClientError> {
        let new_item = new_topic();
        let _ = K8CLIENT.create_item(&new_item).await?;

        let patch = json!({
            "spec": {
                "partitions": 5
            }
        });

        K8CLIENT.patch::<TopicSpec, TopicStatus>(&new_item.metadata, &patch).await?;

        let item: K8Obj<TopicSpec, TopicStatus> =
            K8CLIENT.retrieve_item(&new_item.metadata).await?;
        let spec = item.spec.unwrap();
        assert_eq!(spec.partitions, 5);

        K8CLIENT.delete_item::<TopicSpec>(&new_item.metadata).await?;

        Ok(())
    }

    #[test_async]
    async fn test_client_apply_topic() -> Result<(), ClientError> {
        let mut new_item = new_topic();
        let status = K8CLIENT.apply(&new_item).await?;
        match status {
            ApplyResult::Created(_) => {
                assert!(true, "created");
                // check to ensure item exists
                let _item: K8Obj<TopicSpec, TopicStatus> =
                    K8CLIENT.retrieve_item(&new_item.metadata).await?;
                let spec = new_item.spec.as_mut().unwrap();
                spec.partitions = 5;
                let status = K8CLIENT.apply(&new_item).await?;
                match status {
                    ApplyResult::None => assert!(false, "no change"),
                    ApplyResult::Created(_) => assert!(false, "created, should not happen"),
                    ApplyResult::Patched(_) => {
                        let patch_item: K8Obj<TopicSpec, TopicStatus> =
                            K8CLIENT.retrieve_item(&new_item.metadata).await?;
                        assert_eq!(patch_item.spec.unwrap().partitions, 5);
                    }
                }

                K8CLIENT.delete_item::<TopicSpec>(&new_item.metadata).await?;
            }
            _ => {
                assert!(false, "expected created");
            }
        }

        Ok(())
    }

}
