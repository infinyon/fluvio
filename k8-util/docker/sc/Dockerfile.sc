FROM scratch

COPY    sc-k8-server  /fluvio/sc-k8-server

CMD ["/fluvio/sc-k8-server"]