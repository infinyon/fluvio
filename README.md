<div align="center">
<h1>Fluvio</h1>

<a href="https://fluvio.io" target="_blank">
<strong>The programmable streaming platform for data in motion</strong>
</a>

<br>
<br>

<div>
<!-- CI status -->
<a href="https://github.com/infinyon/fluvio/actions">
<img src="https://github.com/infinyon/fluvio/workflows/CI/badge.svg" alt="CI Status" />
</a>

<!-- Crates.io badge -->
<a href="https://crates.io/crates/fluvio">
<img src="https://img.shields.io/crates/v/fluvio?style=flat" alt="Crates.io version" />
</a>

<!-- docs.rs docs -->
<a href="https://docs.rs/fluvio">
<img src="https://docs.rs/fluvio/badge.svg" alt="Fluvio documentation" />
</a>

<!-- Discord invitation -->
<a href="https://discordapp.com/invite/bBG2dTz">
<img src="https://img.shields.io/discord/695712741381636168.svg?logo=discord&style=flat" alt="chat" />
</a>
</div>

<a href="https://fluvio.io">
<img src=".github/assets/fluvio-overview.svg" alt="A visual of a data pipeline with filter, map, and other streaming operations" />
</a>

<br>
<br>
</div>

Fluvio is a distributed, programmable streaming platform written in Rust.

- **High-performance and scalable**: written in Rust leveraging asynchronous code
- **Cloud-Native**: integrated with Kubernetes for declarative, self-healing ops
- **Programmable**: able to deploy WASM modules for arbitrary inline data processing

## Installation

The [Fluvio CLI] is available for MacOS and Linux, and can be installed with the following one-liner:

[Fluvio CLI]: https://fluvio.io/download

```
curl -fsS https://packages.fluvio.io/v1/install.sh | bash
```

The next step is to set up a Fluvio Cluster, which can be done by

- [Creating a free Fluvio Cloud account](https://cloud.fluvio.io), or
- Launching your own cluster on [MacOS] or [Linux]

[MacOS]: https://fluvio.io/docs/getting-started/mac
[Linux]: https://fluvio.io/docs/getting-started/linux

Spoiler: After installing system dependencies, launching a cluster is as easy as

```
fluvio cluster start
```

## Quick start

Once we have Fluvio installed, we can create a new Topic where we'll
be producing and consuming data.

```
$ fluvio topic create hello-fluvio
topic "hello-fluvio" created
```

Open a consumer to stream records that arrive in the Topic:

```
$ fluvio consume hello-fluvio -B
Consuming records from the beginning of topic 'hello-fluvio'
```

As the prompt says, the consumer is now waiting for records to arrive.

Open a new terminal and produce some data using Fluvio's interactive prompt:

```
$ fluvio produce hello-fluvio
> This is my first record
Ok!
> This is my second record
Ok!
> ^C
```

These records will appear in the consumer window as they're sent!

See the [Producer] and [Consumer] documentation for more details on
sending and receiving records with Fluvio.

[Producer]: https://www.fluvio.io/cli/commands/produce/
[Consumer]: https://www.fluvio.io/cli/commands/consume/

## Programmatic SmartStreams for inline data processing

One of Fluvio's unique features is the ability to compile WebAssembly
modules to perform inline data processing such as filtering. These
"SmartStreams" allow us to write arbitrary logic to manipulate data
inside of Fluvio's streaming engine itself, reducing data traffic
by eliminating the need to pipe data out of the system for computation.

### SmartStreams quick start

We can get started with our very first SmartStream in just a few steps.
We're going to write a filter that accepts some records and rejects
others, just like a `.filter` iterator method.

To start, we're going to use the `cargo-generate` tool to set up our
SmartStream's project template. The template we're using has taken care
of setting up the WASM compilation for our SmartStream. We can install
`cargo-generate` with the following command:

```
cargo install cargo-generate
```

Then, we'll use `cargo-generate` to set up our project. Be sure to
select the `filter` option:

```
$ cargo generate --git https://github.com/infinyon/fluvio-smartstream-template
🤷   Project Name : filter-example
🔧   Creating project called `filter-example`...
🤷   Which type of SmartStream would you like? [filter] [default: filter]: filter
✨   Done! New project created /home/user/filter-example
```

If we move into the new project directory, we can see that there is a starter
`src/lib.rs` with a simple SmartStream already written!

```rust
// src/lib.rs
use fluvio_smartstream::{smartstream, Record};

#[smartstream(filter)]
pub fn filter(record: &Record) -> bool {
    let str_result = std::str::from_utf8(record.value.as_ref());
    let string = match str_result {
        Ok(s) => s,
        _ => return false,
    };

    string.contains('a')
}
```

> Filter SmartStreams take a `Record` as input and must return `true` or `false`
> to keep or discard the record from the stream, respectively.

This particular SmartStream reads the `Record`'s value as a UTF-8 string,
and checks whether that string contains a lower-case `a`. This will make it easy to check
whether the filter is working.

Let's compile our SmartStream and try it out:

```
$ cargo build --release
```

We'll need a Topic to test out the filter, so let's make one of those:

```
fluvio topic create filter-example
```

Now we'll set up our SmartStream. SmartStreams are applied at consume-time, so we
need to pass our WASM module to the `fluvio consume` command, like so:

```
$ fluvio consume filter-example -B --smart-stream=target/wasm32-unknown-unknown/release/filter-example.wasm
Consuming records from the beginning of topic 'hello-fluvio'
```

The consumer will wait here until some Records come along that pass the filter's test.
We can produce some records to test it out:

```
$ fluvio produce filter-example
> apple
Ok!
> APPLE
Ok!
> banana
Ok!
> BANANA
Ok!
> ^C
```

In the consumer window, we should see only records containing `a` come through:

```
apple
banana
```

For a deeper dive into SmartStreams, [see our blog on SmartStream filtering for server logs]!

[see our blog on SmartStream filtering for server logs]: https://www.infinyon.com/blog/2021/06/smartstream-filters/

## Release Status

Fluvio is currently in **Alpha** and is not ready to be used in production.
Our CLI and API interfaces are still in rapid development and may experience
breaking changes at any time. We do our best to adhere to semantic versioning
but until our R1 release we cannot guarantee it.

## Contributing

If you're interested in contributing to Fluvio, [check out our Contributing guide]
to see how to get started, and feel free to [join our community Discord] to ask us
any questions you may have!

[check out our Contributing guide]: ./CONTRIBUTING.md
[join our Community Discord]: https://discordapp.com/invite/bBG2dTz

## License

Fluvio is licensed under the [Apache 2.0 License].

[Apache 2.0 License]: ./LICENSE
