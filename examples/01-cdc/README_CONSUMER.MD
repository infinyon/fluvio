# CDC Consumer

CDC consumer reads events from the **Fluvio streams** and generates database queries to reconstruct identical replicas. A **Fluvio stream** supports any number of consumers, hence a database can have any number of replicas.

CDC consumers have database filters, which allowed them to replicate only a subset of databases from the overall of the overall data set.

## Consumer Profile

CDC consumers read configuration parameters from a profile file. These parameters help the consumer identify the **Fluvio Stream** and the target **mysql database**.  CDC producer do not need to be colocated  with a database, a TCP connection to the database is sufficient.

### Profile file

The profile has the following parameters:

```
##
# File to save last read offset of the Fluvio stream. If consumer restarts, it resumes reading from this offset.
#   required
#       last_offset_file
##
last_offset_file = <offset-file-name>

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
# Fluvio streaming settings
#   defaults:
#       topic: rust-mysql-cdc
##
[fluvio]
topic = <topic-name>
```

### Start Consumer

Provisioning a consumer to watch a local database is a 2 step process:

1) Create a profile to match your database settings
2) Start consumer with custom profile

```
$ ./target/debug/cdc-consumer ./my-consumer-profile.toml
```