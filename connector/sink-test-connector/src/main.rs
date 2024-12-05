mod sink;

use fluvio_connector_common::{
    connector, consumer::ConsumerStream, secret::SecretString, LocalBoxSink, Result, Sink,
};
use futures::{SinkExt, StreamExt};
use sink::TestSink;

#[connector(sink)]
async fn start(config: CustomConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let sink = TestSink::new(&config)?;
    let mut sink = sink.connect(None).await?;

    if config.test_stream_lifetime {
        // This test case ensures the stream is properly dropped, even when it is in a state
        // that would otherwise prevent it from being dropped.
        // The challenge here is that the stream could remain "stuck" in such a state,
        // holding resources indefinitely.
        //
        // The solution is to explicitly shut down the future managing the stream before it
        // gets into this problematic state. Previously, we used a `TakeUntil` wrapper to
        // stop the stream. However, this approach failed when the stream entered a state
        // that blocked it from being dropped correctly.
        //
        // Here, we simulate such a scenario using an infinite loop to test the robustness
        // of the stream's lifetime management.
        loop {
            while let Some(item) = stream.next().await {
                process_item(
                    String::from_utf8(item?.as_ref().to_vec())?,
                    &mut sink,
                    &mut stream,
                )
                .await?;
            }
        }
    }

    while let Some(item) = stream.next().await {
        process_item(
            String::from_utf8(item?.as_ref().to_vec())?,
            &mut sink,
            &mut stream,
        )
        .await?;
    }

    Ok(())
}

async fn process_item(
    str: String,
    sink: &mut LocalBoxSink<String>,
    stream: &mut impl ConsumerStream,
) -> Result<()> {
    sink.send(str).await?;
    stream.offset_commit()?;
    stream.offset_flush().await?;
    Ok(())
}

#[connector(config)]
struct CustomConfig {
    api_key: SecretString,
    client_id: String,
    #[serde(default)]
    test_stream_lifetime: bool,
}
