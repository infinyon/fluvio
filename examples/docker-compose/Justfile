default:
  just --list

start:
  docker compose up --build

stop:
  docker compose down

rm:
  docker compose rm --force

fluvio:
  docker build --tag fluvio .
  docker run --name fluvio -d fluvio sleep infinity

fluvio-ssh:
  docker exec -it fluvio /bin/bash
