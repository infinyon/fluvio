use async_std::task::block_on;
use fluvio_benchmark::{setup, throughput::run_throughput_test};

fn main() {
    let s = setup();

    block_on(run_throughput_test(s));
}
