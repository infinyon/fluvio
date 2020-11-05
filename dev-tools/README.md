# To start local cluster with custom SPU 5001,5002,5003

For SPU 5001
```
$ flvd custom-spu register --id 5001 -p 0.0.0.0:9020 -v  0.0.0.0:9021
$ flvd run spu -i 5001 -p 0.0.0.0:9030 -v 0.0.0.0:9031
```

For SPU 5002
```
$ flvd custom-spu register --id 5002 -p 0.0.0.0:9030 -v  0.0.0.0:9031
$ flvd run spu -i 5002 -p 0.0.0.0:9030 -v 0.0.0.0:9031
```

For SPU 5003
```
$ flvd custom-spu register --id 5003 -p 0.0.0.0:9040 -v  0.0.0.0:9041
$ flvd run spu -i 5003 -p 0.0.0.0:9040 -v 0.0.0.0:9041
```

# Debugging GITHUB Actions

