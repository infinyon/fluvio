# Docker Compose + Fluvio

You can run Fluvio Clusters in a Docker Compose setup, this could be useful for
local development and POC development.

In order to run a Fluvio Cluster through Docker, you will need to run Fluvio
components separately, we can use Docker Compose `service`s to achieve this.

## Services

- `sc: Streaming Controller`
- `sc-setup: Post-Initialization Commands`
- `spu: Streaming Processing Unit`

> To learn more about Fluvio architecture, please refer to [Fluvio Documentation][1]

## Running Locally

Clone this repo using `git clone https://github.com/infinyon/fluvio.git` and
cd into `./fluvio/examples/docker-compose`, then run `docker compose up`.

> Optionally you can run on detached mode `docker compose up -d` so
> Fluvio runs in the background.

Then use the `fluvio` CLI to connect to the cluster running in Docker, to do
that you must set the _Fluvio Profile_ to point to Docker's container SC:

> If you dont have the Fluvio CLI installed, run the following command
> `curl -fsS https://hub.infinyon.cloud/install/install.sh | bash`.
> Refer to [Fluvio CLI Reference][2] for more details.

```bash
fluvio profile add docker 127.0.0.1:9103 docker
```

> Fluvio Streaming Controller (SC) usually runs on port `9003` but given that our
> SC is running in a Docker Container, internal port `9003` is mapped to `9103`
> in your system's network.

With the profile set, you are now able to perform Fluvio Client operations
like listing topics:

```bash
fluvio topic list
```

## Teardown

In order to shutdown the Fluvio Cluster running in Docker, you must issue the
following `compose` command:

```bash
docker compose down
```

> Remember to run this command in the same directory as the `docker-compose.yml`
> file.

[1]: https://www.fluvio.io/docs/fluvio/concepts/architecture/overview/
[2]: https://www.fluvio.io/docs/fluvio/cli/overview
