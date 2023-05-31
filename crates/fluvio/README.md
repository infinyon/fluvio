# Fluvio

# Getting Started

1. Install [Fluvio CLI][Install Fluvio CLI] if you havent already

2. Create a new topic using the CLI

```bash
fluvio topic create "echo-test"
```

3. Create a new cargo project and install `fluvio`, `futures` and `async-std`

```bash
cargo add fluvio
cargo add futures
cargo add async-std --features attributes
```

4. Copy and paste the following snippet into your  `src/main.rs`

```rust
use std::time::Duration;

use fluvio::{Offset, RecordKey};
use futures::StreamExt;

const TOPIC: &str = "echo-test";
const MAX_RECORDS: u8 = 10;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = fluvio::producer(TOPIC).await?;
    let consumer = fluvio::consumer(TOPIC, 0).await?;
    let mut consumed_records: u8 = 0;

    for i in 0..10 {
        producer.send(RecordKey::NULL, format!("Hello from Fluvio {}!", i)).await?;
        println!("[PRODUCER] sent record {}", i);
        async_std::task::sleep(Duration::from_secs(1)).await;
    }

    // Fluvio batches records by default, call flush() when done producing
    // to ensure all records are sent
    producer.flush().await?;

    let mut stream = consumer.stream(Offset::beginning()).await?;

    while let Some(Ok(record)) = stream.next().await {
        let value_str = record.get_value().as_utf8_lossy_string();

        println!("[CONSUMER] Got record: {}", value_str);
        consumed_records += 1;

        if consumed_records >= MAX_RECORDS {
            break;
        }
    }

    Ok(())
}
```

5. Run `cargo run` and expect the following output

```txt
[PRODUCER] sent record 0
[PRODUCER] sent record 1
[PRODUCER] sent record 2
[PRODUCER] sent record 3
[PRODUCER] sent record 4
[PRODUCER] sent record 5
[PRODUCER] sent record 6
[PRODUCER] sent record 7
[PRODUCER] sent record 8
[PRODUCER] sent record 9
[CONSUMER] Got record: Hello, Fluvio 0!
[CONSUMER] Got record: Hello, Fluvio 1!
[CONSUMER] Got record: Hello, Fluvio 2!
[CONSUMER] Got record: Hello, Fluvio 3!
[CONSUMER] Got record: Hello, Fluvio 4!
[CONSUMER] Got record: Hello, Fluvio 5!
[CONSUMER] Got record: Hello, Fluvio 6!
[CONSUMER] Got record: Hello, Fluvio 7!
[CONSUMER] Got record: Hello, Fluvio 8!
[CONSUMER] Got record: Hello, Fluvio 9!
```

6. Clean Up

```bash
fluvio topic delete echo-test
topic "echo-test" deleted
```

[Install Fluvio CLI]: https://www.fluvio.io/cli/
