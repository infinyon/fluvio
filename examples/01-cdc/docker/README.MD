# Mysql - Docker Setup

CDC sample application has a **producer** and a **consumer** that replicate database changes in real-time. **Cdc-producer** must be co-located with the **master** database to read its **binlog** files and generate Fluvio events. **Cdc-consumer** needs remote access to the **slave** database. For additional information, checkout [MYSQL documentation](https://dev.mysql.com/doc/refman/8.0/en/replication-howto-masterbaseconfig.html).


## Prerequisites

* **Mysql Client** - for accessing mysql databases .
* **Docker** - to run two MYSQL servers in parallel.


## Installation Script

Installation script sets-up the **root** password and creates a **fluvio** user.

* user: **root**, password: **root**
* user: **fluvio**, password: **fluvio4cdc!**
  * **fluvio** user has **full privileges** to databases prepended with **flvDb**.

The install script has the following parameters:
 * **-n**: container name
 * **-d**: local data path to map mysql data
 * **-p**: port of the mysql containers (i.e. producer: 3080, consumer: 3090)


## Install Producer

Run installer

```
$ ./install.sh -n mysql-producer -d ~/mysql-cdc/mysql-producer -p 3080
```

Producer mapped directory and port:
* directory: **~/mysql-cdc/mysql-producer**
* port **3080**

Ensure container is running

```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
a672f78e73da        mysql-80            "docker-entrypoint.s…"    1 minutes ago      Up 1 minutes       33060/tcp, 0.0.0.0:3080->3306/tcp   mysql-producer
```

Ensure **binlog** files are visible:

```
$ ls ~/mysql-cdc/mysql-producer/
... 
binlog.000001
binlog.000002
binlog.000003		
binlog.000004
binlog.000005
binlog.index
...
```

Connect to **producer** database:

```
$ mysql -h 0.0.0.0 -P 3080 -ufluvio -pfluvio4cdc!
...
Server version: 8.0.20 MySQL Community Server - GPL
...

mysql> 
```


## Install Consumer

Run installer

```
$ ./install.sh -n mysql-consumer -d ~/mysql-cdc/mysql-consumer -p 3090
```

Consumer mapped directory and port:
* directory: **~/mysql-cdc/mysql-consumer**
* port **3090**


Connect to **consumer** database:

```
$ mysql -h 0.0.0.0 -P 3090 -ufluvio -pfluvio4cdc!
...
Server version: 8.0.20 MySQL Community Server - GPL
...

mysql> 
```

## Next Steps

Your environment is ready to [build and run Mysql-CDC](../README.MD#build-producerconsumer).

## Troubleshooting
This guide helps users to solve issues they might face during the setup process.

### Mac OS X mysql-client Installation


You have installed mysql-client using `brew install mysql-client`, but while trying to connect to producer or consumer database, you encounter the following error:

```
-bash: mysql: command not found
```

This indicates that you need to add mysql bin path to .bash_profile

`export PATH="/usr/local/opt/mysql-client/bin:$PATH"`
