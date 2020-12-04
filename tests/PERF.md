# Generating FlameGraph for SPU

This runs on Ubuntu

## References

- [Perf to Flame Graph](https://www.percona.com/blog/2019/11/20/profiling-software-using-perf-and-flame-graphs/)


## Installation

Clone Flame Graph
```
git clone https://github.com/brendangregg/FlameGraph
```

Install Perf tools
```
sudo apt-get install linux-tools-5.4.0-1029-aws -y
```

## Run Local Fluvio Cluster

Ensure release profile is set to debug to `Cargo.toml`
```
[profile.release]
debug = true
```

```
 make RELEASE=true smoke-test
```

Create a topic
```
./target/release/fluvio  topic create t1
```

## Start Perf for SPU

```
sudo perf record -p $(pgrep -f spu) --call-graph dwarf 
```

## Run produce and consume

```
./target/release/fluvio produce t1 -r $(ls /var/log/journal/**/*.journal)

```

then control-c

## Generate graph

Convert perf data into script
```
sudo perf script > perf.script
```

Generate graph 
```
FlameGraph/stackcollapse-perf.pl perf.script | FlameGraph/flamegraph.pl > flame.svg
```

