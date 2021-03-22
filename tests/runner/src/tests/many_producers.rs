// We want to create 6 producers doing this

//import Fluvio from '@fluvio/client';
//const TOPIC = "test";
//const startServer = async () => {
//    const fluvio = await Fluvio.connect();
//    const sessionProducer = await fluvio.topicProducer(TOPIC);
//    for (var i = 0; i < 10000; i++) {
//        const message = `line-${i + 1} aaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccdddddddddddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeefffffffffffffffffffffffffffggggggggggggggggg`;
//        console.log(message);
//        await sessionProducer.sendRecord(message, 0);
//    }
//};
//startServer();

use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::TestCase;
use fluvio::Fluvio;
use std::sync::Arc;

//#[fluvio_test()]
pub async fn run(_client: Arc<Fluvio>, _opt: TestCase) {
    println!("I'm the many producers test");
}
