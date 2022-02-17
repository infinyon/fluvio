<div align="center">
<h1>Fluvio</h1>
<a href="https://fluvio.io" target="_blank">
 <strong>The programmable data streaming platform</strong>
 </a>
<br>
<br>

<div>
<!-- CI status -->
<a href="https://github.com/infinyon/fluvio/actions/workflows/ci.yml">
<img src="https://github.com/infinyon/fluvio/workflows/CI/badge.svg" alt="CI Status" />
</a>

<!-- CD status -->
<a href="https://github.com/infinyon/fluvio/actions/workflows/cd_dev.yaml">
<img src="https://github.com/infinyon/fluvio/workflows/CD_Dev/badge.svg" alt="CD Status" />
</a>

<a href="https://crates.io/crates/fluvio">
<img src="https://img.shields.io/crates/v/fluvio?style=flat" alt="Crates.io version" />
</a>

<!-- docs.rs docs -->
<a href="https://docs.rs/fluvio">
<img src="https://docs.rs/fluvio/badge.svg" alt="Fluvio documentation" />
</a>

<a href="https://discordapp.com/invite/bBG2dTz">
<img src="https://img.shields.io/discord/695712741381636168.svg?logo=discord&style=flat" alt="chat" />
</a>
</div>

<br>
<a href="https://fluvio.io">
<img src=".github/assets/fluvio-overview.svg" alt="A visual of a data pipeline with filter, map, and other streaming operations" />
</a>


<br>
<br>
</div>

Fluvio is a high-performance distributed data streaming platform that's written
in Rust, built to make it easy to run real-time applications.


## Install Fluvio CLI

```bash
curl -fsS https://packages.fluvio.io/v1/install.sh | bash
```

This will  download and configure Fluvio CLI

Add `$HOME/.fluvio/bin/` to `$PATH` 

```bash
echo 'export PATH="${HOME}/.fluvio/bin:${PATH}"' >> ~/.bashrc
```

For an in-depth introduction to Fluvio please visit [our docs](https://www.fluvio.io/docs/))

## Basic Quick Start

Getting a Fluvio cluster running requires a few tools

* [Fluvio CLI](#install-fluvio-cli) <sup>1</sup>
* Kubernetes <sup>2</sup>
* `helm` <sup>1</sup>
* `kubectl` <sup>1</sup>

<sup>1</sup> Must be installed into PATH<br>
<sup>2</sup> Only support for Kubernetes distros: `minikube`, `k3d`, `kind`  

### Create a cluster

A Fluvio cluster is composed of at least one [Streaming Controller (SC)](https://www.fluvio.io/docs/architecture/sc/) and one [Streaming Processing Unit (SPU)](https://www.fluvio.io/docs/architecture/spu/)

```
$ fluvio cluster start
```

[More about architecture](https://www.fluvio.io/docs/architecture/overview/)

### Create a topic

A topic is the basic primitive for a data stream. They are accessed by producers and consumers.

```
$ fluvio topic create my-topic
```

[More about topics](https://www.fluvio.io/docs/architecture/topic-partitions/)
### Produce to topic

You can add records to topic interactively w/ `fluvio produce`

Press `Enter` to send record, and `Ctrl + c` to exit

```
$ fluvio produce my-topic
> Hello
Ok!
> World
Ok!
> ^C
```

[More about producers](https://www.fluvio.io/docs/clients/producer/)

### Consume from topic

You can read the records from topic interactively w/ `fluvio consume`

If new records are added to `my-topic` while actively consuming, they will be appended below the last record. `Ctrl + c` to exit

```
$ fluvio consume my-topic -B
Consuming records from the beginning of topic 'my-topic'
Hello
World
⠤
^C
```

[More about consumers](https://www.fluvio.io/docs/clients/consumer/)

## Suggestions to check out next

### Smart Modules

Smart Modules provide your data stream a programmable API for inline data manipulation

- [Docs](https://www.fluvio.io/docs/smartmodules/overview/)

### Smart Connectors

Connectors are components that may be deployed to import or export streaming data from or to a third-party data platform

- [Official Connectors repo](https://github.com/infinyon/fluvio-connectors)
- [Docs](https://www.fluvio.io/connectors/)

### Table Format

Table Format is used with `fluvio consume --output_type full_table` to customize tabular output

- [Docs](https://www.fluvio.io/cli/commands/table-format/)

## Use Fluvio in your code 
We have client libraries for the following languages:

- [Rust](https://www.fluvio.io/api/fluvio/rust/)
- [Python](https://www.fluvio.io/api/fluvio/python/)
- [Node.js](https://www.fluvio.io/api/fluvio/node/)
- [Java](https://www.fluvio.io/api/fluvio/java/)
- [Go](https://www.fluvio.io/api/community/go/) (Community supported)

## Quick Links

- [Download](https://www.fluvio.io/download/)
- [Rust API docs](https://docs.rs/fluvio)
- [Python API docs](https://infinyon.github.io/fluvio-client-python/fluvio.html)
- [Node.js API docs](https://infinyon.github.io/fluvio-client-node/)
- [Java API docs](https://infinyon.github.io/fluvio-client-java/com/infinyon/fluvio/package-summary.html)
- [Fluvio CLI docs](https://www.fluvio.io/cli/)
- [Fluvio Architecture](https://www.fluvio.io/docs/architecture/overview/)

## Release Status

Fluvio is currently in Beta and is not ready to be used in production. Our CLI and APIs are also in rapid development and may experience technical issues at any time. If you are interested in participating in our Beta Customer program please email us support@infinyon.com.

## Contributing

If you'd like to contribute to the project, please read our
[Contributing guide](CONTRIBUTING.md).

## License

This project is licensed under the [Apache license](LICENSE).
