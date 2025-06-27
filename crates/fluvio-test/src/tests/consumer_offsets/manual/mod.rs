pub mod beginning;
pub mod end;
pub mod from_end;
pub mod from_absolute;
pub mod from_beginning;

use anyhow::Result;

use fluvio::Fluvio;

use super::{option::TestOffsetStart, utils};

pub async fn run_manual_test(
    client: &Fluvio,
    topic: &str,
    partitions: usize,
    start_offset: TestOffsetStart,
) -> Result<()> {
    println!("Running manual test with start offset: {start_offset:?}");

    match start_offset {
        TestOffsetStart::Beginning => {
            beginning::test_strategy_manual_beginning(client, topic, partitions).await
        }
        TestOffsetStart::FromBeginning => {
            from_beginning::test_strategy_manual_from_beginning(client, topic, partitions).await
        }
        TestOffsetStart::Absolute => {
            from_absolute::test_strategy_manual_from_absolute(client, topic, partitions).await
        }
        TestOffsetStart::FromEnd => {
            from_end::test_strategy_manual_from_end(client, topic, partitions).await
        }
        TestOffsetStart::End => end::test_strategy_manual_end(client, topic, partitions).await,
    }
}
