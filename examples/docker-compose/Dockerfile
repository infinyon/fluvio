FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y curl unzip
RUN curl -fsS https://hub.infinyon.cloud/install/install.sh?ctx=dc | bash

ENV PATH "$PATH:/root/.fluvio/bin"
ENV PATH "$PATH:/root/.fvm/bin"
