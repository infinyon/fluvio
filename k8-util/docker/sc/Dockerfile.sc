FROM alpine

COPY    sc-k8-server  /fluvio/sc-k8-server

CMD ["/fluvio/sc-k8-server"]