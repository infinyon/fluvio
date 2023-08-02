# Step-by-Step Instructions

The installation instructions cover 2 options:
* [Use InfinyOn Cloud](#use-infinyon-cloud)
* [Install Local Cluster](#install-an-upstream-cluster-on-local-machine)

---


# Use InfinyOn Cloud

This starts up cloud or setup a fluvio cluster on your machine TBD:

```

```

**Note:** The `mirroring` cluster is experimental and currently runs in AWS EU region.

---


# Install an Upstream Cluster on Local Machine

Installing the cluster on Linux or Mac requires Kubernetes. Use the following instructions to setup a Kubernetes:

* [Install Rancher Desktop for Mac](https://fluvio.io/docs/get-started/mac/#install-rancher-desktop)
* [Install k3d, kubectl and helm for Linux](https://fluvio.io/docs/get-started/linux/#installing-kubernetes-cluster)

**Note**: Only install Kubernetes, then use the following instructions to run download and run the binaries.

### Create a new directory

We'll be generating configuraiton and boat file, so let' start by creating a new directory.

```bash
mkdir -p local/projects/mirror; cd local/projects/mirror
```

### Download Fluvio Binaries

Downlaad binaries:

```bash
curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION='0.10.15-dev-2+mirroring-7bd6897' bash
```

Make sure to add install `.fluvio/bin` to the `$PATH`as specified in the installation script.

Create an alias for Fluvio Binary:

```bash
alias flvd="fluvio-0.10.15-dev-2+mirroring-7bd6897"
```

### Star Upstream Cluster

Start local cluster:

```bash
flvd cluster start --local
```

Run `flvd cluster status` to make sure all good.

### Create up Topic

We'll use the term `upstream` cluster to represent the collection cluster on Cloud or local machine (depending on your installation).

Create a mirror assignment file:

```bash
echo '[
    "boat1", "boat2"
]' > mirror_assignment.json
```

This will set up topics with two partitions

```bash
flvd topic create boats --mirror-assignment  mirror_assignment.json 
```

List partitions:

```bash
flvd partition list
```

It should display all partitions:

```bash
  TOPIC  PARTITION  LEADER  MIRROR        REPLICAS  RESOLUTION  SIZE  HW  LEO  LRS  FOLLOWER OFFSETS     
  boats  0          5001    Target:boat1  []        Online      0 B   0   0    0    0                 [] 
  boats  1          5001    Target:boat2  []        Online      0 B   0   0    0    0                 [] 
```

### Register Edge clusters

Let's register the edge clusters (boat1, boat2) with the upstream cluster. Make sure you match the`mirror-edge` with the `partion name above:

**Boat 1**:

```bash
flvd cluster remote-cluster register --type mirror-edge boat1
```

**Boat 2**:

```bash
flvd cluster remote-cluster register --type mirror-edge boat2
```

Double check the edge are successfully registered:

```bash
flvd cluster remote-cluster list
```

It should show the following:

```bash
  RemoteCluster  RemoteType   Paired  Status  Last Seen 
  boat2          mirror-edge  -       -       -         
  boat1          mirror-edge  -       -       -         
```

### Generate Metadata for Edge Clusters

Each edge device requires a unique metadata file to connect with the upstream cluster. The metadata file all configuration parameters (security, authorizaiton, mirroring etc.) required for a successful connection.

Genrate the metadata files:

**Boat 1**:

Boat 1 target enpoint is a Raspberry Pi device that we'll provision in the next section.

```bash
flvd cluster remote-cluster metadata export --topic boats --mirror-cluster boat1 --target-endpoint 192.168.79.252 --file boat1.json
```

**Boat 2**:

Boat 2 target endpoint is a Virtual Machine that we'll provision below.

```bash
flvd cluster remote-cluster metadata export --topic boats --mirror-cluster boat2 --target-endpoint host.orb.internal --file boat2.json
```

We'll transfer these file to edge devices in the next section.


---

# Raspberry Pi Section


## Setup Edge Cluster (Raspberry Pi)

Ensure the Raspberry Pi is reachable form your lcoal machine. We'll use this device for `boat1`.
SSH into the device

```bash
ssh fluvio@192.168.79.139
```

### Download metadata file

We'll use the metadata file `boat1.json` that we've exported above to provision this device.
Using the `upstream` terminal, let's scp this file to the raspberry pi:

```bash
scp boat1.json fluvio@192.168.79.139:~
```

### Download Fluvio Binaries

Downlaad binaries:

```bash
curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION='0.10.15-dev-2+mirroring-7bd6897' bash
```

Run `fluvio version` to double check.

### Start Edge cluster

We'll use the metadata to start the `edge cluster` on the Raspberry Pi:

```bash
fluvio cluster start --read-only boat1.json
```

Let's check the partitions:

```bash
fluvio partition list
```

The edge device should show one partition as specified in the metadata file:

```bash
  TOPIC  PARTITION  LEADER  MIRROR                REPLICAS  RESOLUTION  SIZE  HW  LEO  LRS  FOLLOWER OFFSETS     
  boats  0          5001    Source:upstream:5001  []        Online      0 B   0   0    0    0                 [] 
```


## Test Edge to Upstreap (Raspberry Pi)

Let's produce on the Raspberry Pi and consume on the Upstreeam cluster

### Produce to edge cluster

Let's produce to the edge cluster:

```bash
fluvio produce boats
```

```bash
A
B
```

### Consume from upstream cluster

Then consume from upstream cluster:

```bash
flvd boats -B -A
```

```bash
A
B
```

---

## Test Disconnect & Reconnect Clusters (Rapsberry Pi)

Shutdown upstream cluster and check that edge cluster can receive records. Then resume upstream cluster and check that is synchronized and can consume the new records.

#### On upstream cluster

Shutdown upstream cluster to simulate network disconnect:

```bash
flvd cluster shutdown --local
```

### On edge cluster

Produce on edge cluster:

```bash
flve produce boats
```

```
C
D
E
```

### On upstream cluster

Restart the upstream cluster:

```bash
flvd cluster upgrade --local
```

The topic on the upstream cluster should automatically synchronize with the edge cluster.
Let's consume from `boats`: 

```bash
flvd consume boats -B
```

```
A
B
C
D
E
```

### On edge cluster

Shutdown & restart edge cluster:

```bash
fluvio shutdown --local
```

Restart the upstream cluster:

```bash
fluvio cluster upgrade --read-only boat1.json
```

```bash
flve consume boats -B
```

```
A
B
C
D
E
```

Apend more data and note that it continues to the mirror.



---

# Edge (VM) Section

## Start an edge clusters

In this section we'll start an edge cluster in a VM on a local computer. Let's setup the environment.

### Instal OrbStack

We'll use OrbStack for VM management.

1. [Install OrbStack](https://orbstack.dev)

2. Start Ubuntu VM machine.  

3. Click VM to open a terminal in the session.

4. Navigate to your data directory `cd ~/local/projects/mirror`

All files we've generated on the local machines should be visible here.


### Download Fluvio Binaries

Download binaries:

```bash
curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION='0.10.15-dev-2+mirroring-7bd6897' bash
```

Add to path:

```bash
echo 'export PATH="${HOME}/.fluvio/bin:${PATH}"' >> ~/.bashrc
source ~/.bashrc
```

Run `fluvio version` to double check the binary.


### Start Edge cluster

We'll use the metadata `boat2` to start the edge cluster:

```bash
fluvio cluster start --read-only boat2.json
```

Let's check the partitions:

```bash
fluvio partition list
```

The edge device should show one partition as specified in the metadata file:

```bash
  TOPIC  PARTITION  LEADER  MIRROR                REPLICAS  RESOLUTION  SIZE  HW  LEO  LRS  FOLLOWER OFFSETS     
  boats  0          5001    Source:upstream:5001  []        Online      0 B   0   0    0    0                 [] 
```

---


## Test Edge to Upstreap (VM)

Let's produce on VM and consume on the Upstreeam cluster

### Produce to edge cluster

Let's produce to the edge cluster:

```bash
fluvio produce boats
```

```bash
2A
2B
```

### Consume from upstream cluster

Then consume from upstream cluster:

```bash
flvd consume boats --mirror boat2 -B
```

```bash
2A
2B
```

---

## Test Disconnect & Reconnect Clusters (VM)

Shutdown upstream cluster and check that edge cluster can continue producing records. Then start upstream cluster back up and check that is synchronized and can consume the new records.

#### On upstream cluster

Shutdown upstream cluster to simulate network disconnect:

```bash
flvd cluster shutdown --local
kubectl delete spu custom-spu-5001
```

Ensure it's shutdown:

```bash
 flvd cluster status
```

### On edge cluster

Produce on edge cluster:

```bash
fluvio produce boats
```

```
2C
2D
2E
```

### On upstream cluster

Restart the upstream cluster:

```bash
flvd cluster upgrade --local
```

The topic on the upstream cluster should automatically synchronize with the edge cluster.
Let's consume from `boat2`: 

```bash
flvd consume boats --mirror boat2 -B
```

```
2A
2B
2C
2D
2E
```

### On edge cluster

Shutdown & restart edge cluster:

```bash
fluvio shutdown --local
```

Restart the upstream cluster:

```bash
fluvio cluster upgrade --read-only boat2.json
```

```bash
flve consume boats -B
```

```
2A
2B
2C
2D
2E
```

Apend more data and note that it continues to the mirror.

---

## Troubleshooting Tips

Dump Upstream SPU logs

```bash
tail -f /usr/local/var/log/fluvio/spu_log_5001.log
```
