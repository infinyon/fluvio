<div align="center">
  <h1>Fluvio</h1>
  <a href="https://fluvio.io" target="_blank">
    <strong>Composable, Declarative, Stateful Data Streaming System</strong>
  </a>
  <br>
  <br>

[![CI Status](https://github.com/infinyon/fluvio/actions/workflows/hourly.yml/badge.svg)](https://github.com/infinyon/fluvio/actions/workflows/hourly.yml)
  [![CD Status](https://github.com/infinyon/fluvio/workflows/CD_Dev/badge.svg)](https://github.com/infinyon/fluvio/actions/workflows/cd_dev.yaml)
  [![fluvio Crates.io version](https://img.shields.io/crates/v/fluvio?style=flat)](https://crates.io/crates/fluvio)
  [![Fluvio Rust documentation](https://docs.rs/fluvio/badge.svg)](https://docs.rs/fluvio)
  [![Fluvio dependency status](https://deps.rs/repo/github/infinyon/fluvio/status.svg)](https://deps.rs/repo/github/infinyon/fluvio)
  [![Fluvio Discord](https://img.shields.io/discord/695712741381636168.svg?logo=discord&style=flat)](https://discordapp.com/invite/bBG2dTz)

  <br>

  [![An animated visual of fluvio distributed streaming runtime](.github/assets/infinyon-fluvio-sdf.gif)](https://fluvio.io)

  <br>
</div>

**Fluvio** is a lean and mean distributed data streaming engine written in Rust. Combined with **Stateful DataFlow** distributed stream processing framework, Fluvio provides a *unified* *composable* *distributed streaming* and *stream processin*g paradigm for developers. It is the foundation of [InfinyOn Cloud](https://infinyon.cloud/).

## Quick Start - Get started with Fluvio in 2 minutes or less!

### Step 1. Download Fluvio Version Manager:

Fluvio is installed via the **Fluvio Version Manager**, shortened to `fvm`.

To install `fvm`, run the following command:

```bash
curl -fsS https://hub.infinyon.cloud/install/install.sh | bash
```
As part of the initial setup, `fvm` will also install the Fluvio CLI available in the stable channel as of the moment of installation.

Fluvio is stored in `$HOME/.fluvio`, with the executable binaries stored in `$HOME/.fluvio/bin`.

> For the best compatibliity on Windows, InfinyOn recommends WSL2

### Step 2. Start a cluster:

Start cluster on you local machine with the following command:

```bash
fluvio cluster start
```

### Step 3. Create Topic:

The following command will create a topic called hello-fluvio:

```bash
fluvio topic create hello-fluvio
```

### Step 4. Produce to Topic, Consume From Topic:

Produce data to your topic. Run the command first and then type some messages:

```bash
fluvio produce hello-fluvio
> hello fluvio
Ok!
> test message
Ok!
```

Consume data from the topic, Run the following command in a different terminal:

```bash
fluvio consume hello-fluvio -B -d
```

Just like that! You have a local cluster running.

## Using Pre-Build Fluvio Versions

You may want to prefer other Fluvio versions than the latest stable release. You can do so by specifying the version in the `VERSION` environment variable.
**Install Latest Release (as of `master` branch)**

```bash
$ curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION=latest bash
```

**Install Specific Version**

```bash
$ curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION=x.y.z bash
```

#### Check Fluvio Core Documentation
Fluvio documentation will provide additional context on how to use the Fluvio clusters, CLI, clients, a development kits.
- [Fluvio overview](https://www.fluvio.io/docs/fluvio/overview)
- [Fluvio CLI docs home](https://www.fluvio.io/docs/fluvio/cli/overview)
- [Fluvio Architecture](https://www.fluvio.io/docs/fluvio/concepts/architecture/overview)

#### Check Stateful DataFlow Documentation
Stateful DataFlow designed to handle complex data processing workflows, allowing for customization and scalability through various programming languages and system primitives.

- [SDF quickstart](https://www.fluvio.io/sdf/quickstart/)
- [SDF Architecture](https://www.fluvio.io/sdf/concepts/architecture)
- [SDF Examples](https://github.com/infinyon/stateful-dataflows-examples/)

#### Learn how to build custom connectors
Fluvio can connect to practically any system that you can think of.
- For first party systems, fluvio clients can integrate with the edge system or application to source data.
- For third party systems fluvio connectors connect at the protocol level and collects data into fluvio topics.

Out of the box Fluvio has native http, webhook, mqtt, kafka inbound connectors. In terms of outbound connectors out of the box Fluvio supports http, SQL, kafka, and experimental builds of DuckDB, Redis, S3, Graphite etc.

Using Connector Development Kit, its intuitive to build connectors to any system fast.

Check out the docs and let us know if you need help building any connector.
- [Connector docs](https://www.fluvio.io/docs/connectors/overview)
- [Connector Development Kit (cdk) docs](https://www.fluvio.io/docs/connectors/cdk)

#### Learn how to build custom smart modules
Fluvio applies wasm based stream processing and data transformations. We call these reusable transformation functions smart modules. Reusable Smart modules are built using Smart Module Development Kit and can be distributed using InfinyOn Cloud hub.

- [Smart Modules docs](https://www.fluvio.io/docs/smartmodules/overview)
- [Smart Modules Development Kit (smdk) docs](https://www.fluvio.io/docs/smartmodules/smdk)

#### Try workflows on InfinyOn Cloud
InfinyOn Cloud is Fluvio on the cloud as a managed service.
- [Check InfinyOn Cloud Guides](https://infinyon.com/docs/guides/)
- [Check out experimental data flows on InfinyOn Labs Repo](https://github.com/infinyon/labs-projects)

### Clients
- [Fluvio Client API docs home](https://www.fluvio.io/docs/fluvio/apis/overview)

**Language Specifc API docs:**
- [Rust API docs](https://docs.rs/fluvio/latest/fluvio/)
- [Python API docs](https://infinyon.github.io/fluvio-client-python/fluvio.html)
- [Javascript API docs](https://infinyon.github.io/fluvio-client-node/)


**Community Maintained:**
- [Go API docs](https://github.com/avinassh/fluvio-go)
- [Java API docs](https://github.com/infinyon/fluvio-client-java)
- [Elixir API docs](https://github.com/viniarck/fluvio-ex)

## Contributing

If you'd like to contribute to the project, please read our
[Contributing guide](CONTRIBUTING.md).

## Community

Many fluvio users and developers have made projects to share with the community.
Here a a few listed below:

### Projects Using Fluvio
- [Swiftide Project](https://github.com/bosun-ai/swiftide): a Rust native library for building LLM applications
- [Real Time Stock Charts](https://github.com/KeptCodes/stock-charts): See how Fluvio is used to update real time stock charts

### Community Connectors
- [Qdrant Connector](https://qdrant.tech/documentation/data-management/fluvio/)
- [Google Sheets Connector](https://github.com/fluvio-connectors/sheets-connector): Send data from Fluvio to Google Sheets
- [Elastic Connector](https://github.com/fluvio-connectors/elastic-connector): Send data from Fluvio to Elastic Search

## Community Development Resources

More projects and utilities are available in the  [Fluvio Community Github Org](https://github.com/fluvio-community/)

- [Hello World Fluvio Connector](https://github.com/fluvio-community/connector-hello-source): Sample Fluvio connector template to build your own connector
- [Gurubase](https://gurubase.io/g/fluvio): Third-party AI/LLM Docs query

### Contributors are awesome
<a href="https://github.com/infinyon/fluvio/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=infinyon/fluvio" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

## Community

Many fluvio users and developers have made projects to share with the community.
Here a a few listed below:

### Projects Using Fluvio
- [Real Time Stock Charts](https://github.com/KeptCodes/stock-charts): See how Fluvio is used to update real time stock charts
- [Google Sheets Connector](https://github.com/fluvio-connectors/sheets-connector): Send data from Fluvio to Google Sheets
- [Elastic Connector](https://github.com/fluvio-connectors/elastic-connector): Send data from Fluvio to Elastic Search
- [Hello World Fluvio Connector](https://github.com/infinyon/connector-hello-source): Sample Fluvio connector template to build your own connector


## License

This project is licensed under the [Apache license](LICENSE).
