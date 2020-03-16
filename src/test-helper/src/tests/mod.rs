mod consume;
mod produce;


/// test consumer
pub fn test_consumer(option: crate::TestOption) {

    use flv_future_aio::task::run_block_on;
    use futures::future::join_all;
    use futures::future::join;


    let mut consumer_test = vec ![];

    for i in 0..option.replication {
        consumer_test.push(consume::validate_consume_message(i));
    }

    run_block_on(
        join(
            produce::produce_message(),
            join_all(consumer_test)
        ));


}