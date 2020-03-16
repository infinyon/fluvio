/// This assumes you have set up the CRD for topics
#[cfg(feature = "k8")]
#[cfg(not(feature = "k8_stream"))]
mod integration_tests {

    use log::debug;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use flv_future_core::test_async;
    use k8_client::fixture::TEST_NS;
    use k8_metadata::client::ApplyResult;
    use k8_client::ClientError;
    use k8_metadata_core::metadata::InputK8Obj;
    use k8_metadata_core::metadata::InputObjectMeta;
    use k8_metadata_core::metadata::UpdateK8ObjStatus;
    use k8_metadata::client::MetadataClient;
    use k8_client::K8Client;
    use k8_client::pod::PodSpec;
    use k8_metadata_core::metadata::K8Obj;
    use k8_metadata_core::Spec;
    use k8_metadata::topic::{TopicSpec, TopicStatus, TopicStatusResolution};

    // way to get static lifetime which is requirement for cluster
    fn create_client() -> K8Client {
        K8Client::default().expect("cluster not initialized")
    }

    fn new_topic() -> InputK8Obj<TopicSpec> {
        let rng = thread_rng();
        let rname: String = rng.sample_iter(&Alphanumeric).take(5).collect();
        let name = format!("test{}", rname);
        debug!("create topic with name: {}", &name);
        let topic_spec = TopicSpec {
            partitions: Some(2),
            replication_factor: Some(5),
            ..Default::default()
        };

        let new_item: InputK8Obj<TopicSpec> = InputK8Obj {
            api_version: TopicSpec::api_version(),
            kind: TopicSpec::kind(),
            metadata: InputObjectMeta::named(name.to_lowercase(), TEST_NS.to_owned()),
            spec: topic_spec,
            ..Default::default()
        };

        new_item
    }

    /// get topics.  this assumes there exists topic named "topic1"
    #[test_async]
    async fn test_client_get_topics() -> Result<(), ClientError> {
        let client = create_client();
        let topics = client.retrieve_items::<TopicSpec>(TEST_NS).await?;
        assert!(topics.items.len() > 0);
        assert_eq!(topics.kind, "TopicList");
        let topic = &topics.items[0];
        assert_eq!(topic.kind, "Topic");
        assert_eq!(topic.metadata.name, "topic1");
        assert_eq!(topic.spec.partitions, Some(1));
        Ok(())
    }

    /// get specific topic named topic1.  this assumes there exists topic named "topic1"
    #[test_async]
    async fn test_client_get_single_topic() -> Result<(), ClientError> {
        let client = create_client();
        let topic: K8Obj<TopicSpec, TopicStatus> = client
            .retrieve_item(&InputObjectMeta::named("topic1", TEST_NS))
            .await?;
        assert_eq!(topic.kind, "Topic");
        assert_eq!(topic.metadata.name, "topic1");
        assert_eq!(topic.spec.partitions, Some(1));

        Ok(())
    }

    /// create and delete topic
    #[test_async]
    async fn test_client_create_and_delete_topic() -> Result<(), ClientError> {
        let new_item = new_topic();
        debug!("creating and delete topic:  {}", new_item.metadata.name);
        let client = create_client();
        let created_item = client.create_item::<TopicSpec>(new_item).await?;
        client
            .delete_item::<TopicSpec, _>(&created_item.metadata.as_input())
            .await?;
        Ok(())
    }

    #[test_async]
    async fn test_client_create_update_status() -> Result<(), ClientError> {
        let new_item = new_topic();
        let client = create_client();
        let topic = client.create_item::<TopicSpec>(new_item).await?;

        assert!(topic.status.is_none());
        // create simple status
        let status = TopicStatus {
            resolution: TopicStatusResolution::Init,
            ..Default::default()
        };

        let update_status = UpdateK8ObjStatus::new(status, topic.metadata.as_update());
        client.update_status::<TopicSpec>(&update_status).await?;

        let exist_topic: K8Obj<TopicSpec, TopicStatus> =
            client.retrieve_item(&topic.metadata.as_input()).await?;

        let test_status = exist_topic.status.expect("status should exists");
        assert_eq!(test_status.resolution, TopicStatusResolution::Init);

        client
            .delete_item::<TopicSpec, _>(&exist_topic.metadata.as_input())
            .await?;

        Ok(())
    }

    #[test_async]
    async fn test_client_get_pods() -> Result<(), ClientError> {
        let client = create_client();
        client.retrieve_items::<PodSpec>(TEST_NS).await?;
        Ok(())
    }

    #[test_async]
    async fn test_client_apply_topic() -> Result<(), ClientError> {
        let topic = new_topic();
        let client = create_client();
        match client.apply(topic).await? {
            ApplyResult::Created(new_topic) => {
                assert!(true, "created");
                // check to ensure item exists
                client
                    .exists::<TopicSpec, _>(&new_topic.metadata.as_input())
                    .await?;
                let mut update_topic = new_topic.as_input();
                update_topic.spec.partitions = Some(5);
                match client.apply(update_topic).await? {
                    ApplyResult::None => assert!(false, "no change"),
                    ApplyResult::Created(_) => assert!(false, "created, should not happen"),
                    ApplyResult::Patched(_) => {
                        let patch_item: K8Obj<TopicSpec, TopicStatus> =
                            client.retrieve_item(&new_topic.metadata.as_input()).await?;
                        assert_eq!(patch_item.spec.partitions, Some(5));
                    }
                }

                client
                    .delete_item::<TopicSpec, _>(&new_topic.metadata.as_input())
                    .await?;
            }
            _ => {
                assert!(false, "expected created");
            }
        }

        Ok(())
    }
}
