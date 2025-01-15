FROM debian:stable-slim

RUN apt-get update && apt-get -y upgrade
RUN apt-get install -y tini
RUN apt-get install -y curl

# setup fluvio as non user
ENV USER=fluvio
RUN adduser \
    --disabled-password \
    --home "/home/$USER" \
    "$USER"
USER $USER
WORKDIR /home/fluvio


# install fluvio
RUN curl -fsS https://hub.infinyon.cloud/install/install.sh | bash
RUN ls -la /home/fluvio/.fluvio/bin
ENV PATH="/home/fluvio/.fluvio/bin:$PATH"
RUN export PATH
RUN fluvio version

# copy fluvio profile and test
COPY ./fluvio_profile.toml /home/fluvio/.fluvio/config
# RUN fluvio topic list

WORKDIR /home/fluvio/connector


# Download specific connector, you can configure this to download the connector you need
RUN cdk hub download infinyon/http-source@0.3.8
RUN tar xf infinyon-http-source-0.3.8.ipkg
RUN tar -xzf manifest.tar.gz
RUN ls -la


# Copy connector configuration
COPY ./connector.yaml /home/fluvio/connector
RUN ls -la /home/fluvio/connector

# run http-source, this will be different for each connector
ENTRYPOINT ["/home/fluvio/connector/http-source",  "--config", "/home/fluvio/connector/connector.yaml"]
