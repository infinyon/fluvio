FROM alpine

COPY fluvio-sc-k8 /fluvio/fluvio-sc-k8
CMD ["/fluvio/fluvio-sc-k8"]
