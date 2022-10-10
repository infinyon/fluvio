
#
# This dockerfile creates a container of development dependencies for Fluvio smartmodules
# It configures a container, sets up an example smartmodule project, and builds the example
#
# to build:
# docker build -t sm-dev . -f smartmodule.Dockerfile
#
# to run: (opens a shall)
# docker run -it sm-dev
#


ARG ARCH=
FROM ${ARCH}ubuntu:22.04
RUN apt-get update -y

RUN apt-get install -y \
	curl \
	make \
	gcc \
	perl \
	git

# add user
RUN useradd -ms /bin/bash smdevel

# optionally add sudo
# RUN apt-get install -y sudo
# RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
# RUN usermod -aG sudo smdevel

# optionally add vim
# RUN apt-get install -y vim

USER smdevel
ENV USER=smdevel
WORKDIR /home/smdevel
SHELL ["/usr/bin/bash", "-c"]

# setup rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN source "$HOME/.cargo/env"

# setup fluvio
RUN curl -fsS https://packages.fluvio.io/v1/install.sh | bash

# add Fluvio smartmodule deps
# source cargo/env is a little bit of a workaround
RUN source "$HOME/.cargo/env" && rustup target install wasm32-unknown-unknown
RUN source "$HOME/.cargo/env" && cargo install cargo-generate

# create example-sm dir with a template project in it
#   a user usually will run "cargo generate gh:infinyon/fluvio-smartmodule-template" and provide inputs
#   here the script provides interactive parameters in tmpl-in
RUN echo '[values]' > tmpl-in
RUN echo 'smartmodule-type = "filter"' >> tmpl-in
RUN echo 'smartmodule-params = "true"' >> tmpl-in
RUN source "$HOME/.cargo/env" && \
	cargo generate gh:infinyon/fluvio-smartmodule-template --name example-sm --template-values-file tmpl-in
RUN cd example-sm && source "$HOME/.cargo/env" && cargo build --release

CMD bash
