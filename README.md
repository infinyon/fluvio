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

## Quick Start - Get started with Fluvio and Stateful DataFlow in 5 minutes or less on your system!

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

### Step 3. Install SDF CLI

Stateful dataflows are managed via `sdf cli` that we install it using `fvm`.

```bash
fvm install sdf-beta6
```

### Step 4. Create the Dataflow file

Create a dataflow file in the directory `split-sentence` directory:

```bash
mkdir -p split-sentence-inline
cd split-sentence-inline
```

Create the `dataflow.yaml` and add the following content:

```yaml
apiVersion: 0.5.0

meta:
  name: split-sentence-inline
  version: 0.1.0
  namespace: example

config:
  converter: raw

topics:
  sentence:
    schema:
      value:
        type: string
        converter: raw
  words:
    schema:
      value:
        type: string
        converter: raw

services:
  sentence-words:
    sources:
      - type: topic
        id: sentence

    transforms:
      - operator: flat-map
        run: |
          fn sentence_to_words(sentence: String) -> Result<Vec<String>> {
            Ok(sentence.split_whitespace().map(String::from).collect())
          }
      - operator: map
        run: |
          pub fn augment_count(word: String) -> Result<String> {
            Ok(format!("{}({})", word, word.chars().count()))
          }

    sinks:
      - type: topic
        id: words
```

### Step 5. Run the DataFlow

Use sdf command line tool to run the dataflow:

```bash
sdf run --ui
```
> The --ui flag serves the graphical representation of the dataflow on SDF Studio.

### Step 6. Test the DataFlow

Produce sentences to in `sentence` topic:
```bash
fluvio produce sentence
```
Input some text, for example:
```bash
Hello world
Hi there
```
Consume from `words` to retrieve the result:
```bash
fluvio consume words -Bd
```
See the results, for example:
```bash
Hello(1)
world(1)
Hi(1)
there(1)
```

### Step 6. Inspect State

The dataflow collects runtime metrics that you can inspect in the runtime terminal.

Check the sentence-to-words counters:
```bash
show state sentence-words/sentence-to-words/metrics
```
See results, for example:
```bash
 Key    Window  succeeded  failed
 stats  *       2          0
 ```
 Check the augment-count counters:
 ```bash
 show state sentence-words/augment-count/metrics
 ```
 See results, for example:
 ```bash
 Key    Window  succeeded  failed
 stats  *       4          0
 ```
Congratulations! You've successfully built and run a composable dataflow!

More examples of Stateful DataFlow are on GitHub - https://github.com/infinyon/stateful-dataflows-examples/.

#### Check Fluvio Core Documentation
Fluvio documentation will provide additional context on how to use the Fluvio clusters, CLI, clients, a development kits.
- [Fluvio overview](https://www.fluvio.io/docs/fluvio/overview)
- [Fluvio CLI docs home](https://www.fluvio.io/docs/fluvio/cli/overview)
- [Fluvio Architecture](https://www.fluvio.io/docs/fluvio/concepts/architecture/overview)

#### Check Stateful DataFlow Documentation
Stateful DataFlow designed to handle complex data processing workflows, allowing for customization and scalability through various programming languages and system primitives.

- [SDF overview](https://www.fluvio.io/sdf/)
- [SDF Architecture](https://www.fluvio.io/sdf/concepts/architecture)

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

## License

This project is licensed under the [Apache license](LICENSE).
