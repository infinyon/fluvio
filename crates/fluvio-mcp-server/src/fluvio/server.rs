use anyhow::Result;
use fluvio::{
    metadata::{objects::ListRequest, topic::TopicSpec},
    Fluvio,
};
use fluvio_sc_schema::partition::PartitionSpec;
use poem_mcpserver::{stdio::stdio, tool::Text, McpServer, Tools};

use super::model::McpTopicConfig;

pub(crate) struct FluvioMcpServer {
    fluvio: Fluvio,
}

impl FluvioMcpServer {
    pub(crate) async fn start_stdio(fluvio: Fluvio) -> Result<()> {
        let server = FluvioMcpServer { fluvio };
        stdio(McpServer::new().tools(server))
            .await
            .map_err(|e| e.into())
    }
}

#[Tools]
impl FluvioMcpServer {
    async fn list_topics(&self) -> Result<Text<String>> {
        let admin = self.fluvio.admin().await;
        let topics = admin
            .list_with_config::<TopicSpec, String>(ListRequest::default().system(false))
            .await?;
        Ok(Text(serde_json::to_string(&topics)?))
    }

    async fn list_partitions(&self) -> Result<Text<String>> {
        let admin = self.fluvio.admin().await;
        let partitions = admin
            .list_with_config::<PartitionSpec, String>(ListRequest::default().system(false))
            .await?;
        Ok(Text(serde_json::to_string(&partitions)?))
    }

    async fn create_topic(&self, name: String, config: McpTopicConfig) -> Result<Text<String>> {
        let admin = self.fluvio.admin().await;
        let spec: TopicSpec = config.try_into()?;
        let output = admin.create(name, false, spec).await?;
        Ok(Text(serde_json::to_string(&output)?))
    }
}
