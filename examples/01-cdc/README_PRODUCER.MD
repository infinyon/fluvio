# CDC Producer

Mysql logs database modifications in binary log files called [binlog files](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html). CDC producer parses these log entries into events and writes them to **Fluvio streams**.

## MYSQL Configuration

CDC producer requires that the following MYSQL parameters are configured:

```
server-id        = <globally unique id: 1-2^32>
binlog_format    = row
log_bin          = /var/lib/mysql/binlog.index
```

Additional parameters may be configured as desired. [MYSQL Reference Manual](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html).

## Producer Profile

CDC producers read configuration parameters from a profile file. These parameters help the producer glue the **binlog** files with **mysql server instance** and the **Fluvio stream**. The producer uses Mysql server instance to lookup table column names that MySQl chose to leave out of the binlog files.

### Profile file

The profile has the following parameters:

```
##
# Universal Resource Name - urn - used to construct URI: flv://<urn>/<database>/<table>
#   required
#       mysql_resource_name
##
mysql_resource_name = <unique-instance-identifier>

##
# Binlog Index file - path of the MYSQL binlog index file
#   required
#       binlog_index_file
##
binlog_index_file = <file-path>

##
# Mysql database settings
#   required:
#       ip_or_host
#       user
#   defaults:
#       password: None
#       port: 3306
##
[database]
ip_or_host = <IP or host>
port = <port number>
user = <username>
password = <password>

##
# Filters to include or exclude databases:
#   defaults: none
##
[filters]
include_dbs = [<db-name>, ...]

##
# Fluvio streaming settings (defaults):
#   defaults:
#       topic: rust-mysql-cdc
#       replica: 1
## 
[fluvio]
topic = <topic-name>
replicas = <replication-factor>
```

### Start Producer

Provisioning a producer to watch a local database is a 2 step process:

1) Create a profile to match your database settings
2) Start producer with custom profile

```
$ ./target/debug/cdc-producer ./my-producer-profile.toml
```