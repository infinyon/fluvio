# Overview

This directory contains scripts to run in a continuous integration (CI) environment.


## Creating diagrams

[Kroki](https://docs.kroki.io/kroki/) is used to generate images from the plaintext diagrams (such as Excalidraw).

To start an instance locally with docker, you can run:

```shell
$ cd docker/kroki
$ docker compose up -d
```

Kroki will be available at http://localhost:8000/


See [Kroki install docs](https://docs.kroki.io/kroki/setup/install/) for more installation options

The Kroki CLI is the main way to generate images against the Kroki container instance

```shell
$ cargo run --bin ci-kroki
```