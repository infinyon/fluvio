use fluvio::{self, Fluvio, FluvioError, Offset, PartitionSelectionStrategy, consumer::Record};
use futures_util::Stream;

pub fn fetch(client: &Fluvio, topic: &str) -> impl Stream<Item = Result<Record, FluvioError>> {
    let consumer = client.consumer(PartitionSelectionStrategy::All(topic.to_owned())).expect("consumer failed");

    let stream = consumer.stream(Offset::end()).expect("stream error");

    stream

}