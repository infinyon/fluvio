To run dns lookup utility, first set up DNS pod by
```kubectl apply -f busybox.yaml```

To get dns entries,
```./dns.sh  <dns-name>```

For example, to get dns entries for spu
```bash
./dns.sh spu.default.svc.cluster.local
Server:    10.96.0.10
Address 1: 10.96.0.10 kube-dns.kube-system.svc.cluster.local

Name:      spu.default.svc.cluster.local
Address 1: 172.17.0.2 spu-0.spu.default.svc.cluster.local
Address 2: 172.17.0.3 spu-1.spu.default.svc.cluster.local
Address 3: 172.17.0.6 spu-2.spu.default.svc.cluster.local
```