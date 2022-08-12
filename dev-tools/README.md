

# Dockerfile for SmartModule development

The dev-tools/smartmodule.Dockerfile creates a container for a development environment supporting developing rust smartmodules.

Build the container with:
    
    $ cd dev-tools
    $ docker build -t sm-dev . -f smartmodule.Dockerfile

The dockerfile will install rust and other development dependencies, including creating and building an initial example project. Start the container with:

    $ docker run -it sm-dev

To share a host folder with the container start the container with:

    $ docker run -it -v <host_abs_path>:<container_path> sm-dev

For example:

    $ docker run -it -v `pwd`/shareme:/home/smdevel/shared sm-dev


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

# Showing TCP states in Linux

This shows tcp states.
```
$ netstat -nat | awk '{print $6}' | sort | uniq -c | sort -r
   2244 TIME_WAIT
     90 ESTABLISHED
     18 LISTEN
      1 established)
      1 Foreign
      1 FIN_WAIT2
      1 FIN_WAIT1
```

Note the high count of [`TIME_WAIT` ](https://serverfault.com/questions/23385/huge-amount-of-time-wait-connections-says-netstat)

```
IME_WAIT is normal. It's a state after a socket has closed, used by the kernel to keep track of packets which may have got lost and turned up late to the party. A high number of TIME_WAIT connections is a symptom of getting lots of short lived connections, not nothing to worry about.

```

# SPU debugging

To show log size:
```
kubectl exec fluvio-spg-main-0 -- ls -lh /var/lib/fluvio/data/spu-logs-0/<topic-name>-0/00000000000000000000.log
```

