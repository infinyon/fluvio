# Change Data Capture (CDC) Demo App - for Fluvio Streaming Platform

**Change Data Capture (CDC)** converts MySQL database changes into real-time events
that can be used to improve replication, keep audit trails, and feed downstream
business intelligence tools.

This example showcases database replication, where a CDC producer publishes
database events to a Fluvio topic, and one or more CDC consumers read the events
and create identical database replicas.

## Requirements

This example requires you to have completed the following prerequisites:

- Install [Docker]
- Install the [mysql client]
- Complete the [Fluvio getting started guide]
  - Note: You may use [Fluvio Cloud] or install [Fluvio locally], but you don't need both

[Docker]: https://docs.docker.com/engine/install/
[Fluvio getting started guide]: https://fluvio.io/docs/getting-started
[Fluvio Cloud]: https://fluvio.io/docs/getting-started/fluvio-cloud/
[Fluvio locally]: https://fluvio.io/docs/getting-started/fluvio-local/

## Docker MySQL deployment

This example requires two instances of MySQL to be up and running: One to be a
leader that produces events, and one to be a follower that consumes events.
We've provided docker images to help you configure these MySQL instances with
the right configurations. Run the following commands to start your MySQL containers:

```bash
$ cd docker/
$ ./install.sh -n mysql-producer -d ~/mysql-cdc/mysql-producer -p 3080
$ ./install.sh -n mysql-consumer -d ~/mysql-cdc/mysql-consumer -p 3090
```

At this point, you should be able to see both docker containers running

```bash
$ docker ps
CONTAINER ID   IMAGE      COMMAND                  CREATED       STATUS       PORTS                               NAMES
17c60cbbfc09   mysql-80   "docker-entrypoint.s…"   3 hours ago   Up 3 hours   33060/tcp, 0.0.0.0:3090->3306/tcp   mysql-consumer
f5ec27a58476   mysql-80   "docker-entrypoint.s…"   3 hours ago   Up 3 hours   33060/tcp, 0.0.0.0:3080->3306/tcp   mysql-producer
```

## Creating a Fluvio Topic

In Fluvio, every message is sent to a Topic. A topic is a sort of category for events that
are related. For this example, we're going to create a topic that will receive all of our
MySQL events. Run the following command to create the topic:

```bash
$ fluvio topic create rust-mysql-cdc
```

## Start the Producer and Consumer

Now we'll launch the CDC Producer, which will watch for any SQL commands that are executed
in the leader MySQL instance and produce corresponding Fluvio events.

```bash
$ cargo run --bin cdc-producer -- examples/01-cdc/producer_profile.toml
```

In another terminal window, we'll launch the CDC Consumer, which listens for new Fluvio
events and replicates them in the follower MySQL instance.

```bash
$ cargo run --bin cdc-consumer -- examples/01-cdc/consumer_profile.toml
```

## Connect to Mysql

Now you're ready to start interacting with your databases. We'll open up
the `mysql` command line and start running queries on the leader database, and we'll
open a separate window connected to the follower database to see that all the changes
get propagated.

### Connect to Producer (leader DB)

The leader database is bound to port 3080. You can connect to it using the `mysql`
command as follows:

```
$ mysql -h 0.0.0.0 -P 3080 -ufluvio -pfluvio4cdc!
...
mysql >
```

The producer profile has a filter that registers only changes applied to the "flvDB"
database. Let's create the database now:

```
mysql> CREATE DATABASE flvDb;
Query OK, 1 row affected (0.01 sec)

mysql> use flvDb;
Database changed
```

### Connect to Consumer (follower DB)

The follower database is bound to port 3090. Open another terminal window and connect
to it using the `mysql` command:

```
$ mysql -h 0.0.0.0 -P 3090 -ufluvio -pfluvio4cdc!
...
mysql >
```

Since the CDC Consumer is already running, the "flvDb" database we just created on
the leader will also be created on the follower!

```
mysql> SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| flvDb              |
| information_schema |
+--------------------+
2 rows in set (0.00 sec)

mysql> use flvDb;
Database changed
```

## MYSQL Test Commands

In the producer terminal, generate mysql commands and see then propagated to the consumer database;

### Producer

```
mysql> CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20), species VARCHAR(20), sex CHAR(1), birth DATE);
Query OK, 0 rows affected (0.03 sec)

mysql> show tables;
+-----------------+
| Tables_in_flbDb |
+-----------------+
| pet             |
+-----------------+
1 row in set (0.00 sec)
```

### Consumer

Create table has been propagated to the consumer:

```
mysql> show tables;
+-----------------+
| Tables_in_flbDb |
+-----------------+
| pet             |
+-----------------+
1 row in set (0.00 sec)
```

## Sample MYSQL Commands

For additional mysql commands, checkout [MYSQL-COMMANDS](./MYSQL-COMMANDS.MD)


## Fluvio Events

CDC producer generates the events on the **Fluvio topic** as defined in the producer profile. By default the topic name is **rust-mysql-cdc**. 

To view the events generated by the CDC producer, start run **fluvio consumer** command:

```
$ ./target/debug/fluvio consume rust-mysql-cdc -B
...
{"uri":"flv://mysql-srv1/flvDb/year","sequence":30,"bn_file":{"fileName":"binlog.000003","offset":10650},"columns":["y"],"operation":{"Add":{"rows":[{"cols":[{"Year":1998}]}]}}}
{"uri":"flv://mysql-srv1/flvDb/year","sequence":31,"bn_file":{"fileName":"binlog.000003","offset":10921},"columns":["y"],"operation":{"Add":{"rows":[{"cols":[{"Year":1999}]}]}}}
{"uri":"flv://mysql-srv1/flvDb/year","sequence":32,"bn_file":{"fileName":"binlog.000003","offset":11192},"columns":["y"],"operation":{"Delete":{"rows":[{"cols":[{"Year":1998}]},{"cols":[{"Year":1999}]}]}}}
```
