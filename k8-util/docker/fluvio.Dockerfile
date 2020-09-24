FROM alpine:3.12

COPY fluvio fluvio

ENTRYPOINT ["fluvio"]