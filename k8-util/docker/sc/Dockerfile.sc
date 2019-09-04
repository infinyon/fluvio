FROM scratch

COPY    sc-server  /fluvio/sc-server

CMD ["/fluvio/sc-server"]