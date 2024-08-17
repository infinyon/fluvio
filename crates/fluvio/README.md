<div align="center">
  <h1>Fluvio</h1>
  <a href="https://fluvio.io" target="_blank">
    <strong>The programmable data streaming platform</strong>
  </a>
</div>

<div align="center">

  [![CI Status](https://github.com/infinyon/fluvio/workflows/CI/badge.svg)](https://github.com/infinyon/fluvio/actions/workflows/ci.yml)
  [![CD Status](https://github.com/infinyon/fluvio/workflows/CD_Dev/badge.svg)](https://github.com/infinyon/fluvio/actions/workflows/cd_dev.yaml)
  [![fluvio Crates.io version](https://img.shields.io/crates/v/fluvio?style=flat)](https://crates.io/crates/fluvio)
  [![Fluvio client API documentation](https://docs.rs/fluvio/badge.svg)](https://docs.rs/fluvio)
  [![Fluvio dependency status](https://deps.rs/repo/github/infinyon/fluvio/status.svg)](https://deps.rs/repo/github/infinyon/fluvio)
  [![Fluvio Discord](https://img.shields.io/discord/695712741381636168.svg?logo=discord&style=flat)](https://discordapp.com/invite/bBG2dTz)

</div>

## What's Fluvio?

Fluvio is a programmable data streaming platform written in Rust. With Fluvio
you can create performant real time applications that scale.

Read more about Fluvio in the [official website][Fluvio.io].

## Getting Started

Let's write a very simple solution with Fluvio, in the following demostration
we will create a topic using the Fluvio CLI and then we wisll produce some
records on this topic. Finally these records will be consumed from the topic
and printed to the stdout.

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

```ignore
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

## Learn More

- [Read on tutorials][Tutorials] to get the most from Fluvio and InfinyOn Cloud
  to scale your streaming solution.

- You can use Fluvio to send or receive records from different sources using [Connectors][Connectors].

- If you want to filter or transform records on the fly read more about [SmartModules][SmartModules].

[Fluvio.io]: https://www.fluvio.io
[Install Fluvio CLI]: https://www.fluvio.io/docs/fluvio/cli/overview
[Connectors]: https://www.fluvio.io/docs/connectors/overview
[SmartModules]: https://www.fluvio.io/docs/smartmodules/overview
[Tutorials]: https://www.fluvio.io/docs/cloud/tutorials/
