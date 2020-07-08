---
title: Fluvio Profiles
menu: Profiles
toc: true
weight: 20
---

Fluvio **profiles** makes managing multiple deployments simple. A **profile** is a .toml configuration file that stores the location of the services. The syntax is as follows:

```bash
version = <profile-version>

[sc]
host = <hostname/ip>
port = <port>

[spu]
host = <hostname/ip>
port = <port>

[kf]
host = <hostname/ip>
port = <port>
```

The parameters are as follows:

* **version** is currently set to "1.0".
* **hostname/ip** is the location of the service, it may be a domain name or an IP address.
* **port** is the listening port of the service.

~> While it is possible to configure all three services, it is not a useful configuration. Services with lower priority are shadowed by the services with higher priority. The lookup order is: SC => SPU => KF

The most common configuration is _one service per profile_.

```toml
version = "1.0"

[sc]
host = "sc.fluvio.dev.acme.com"
port = 9003
```
The configuraton file is stord at `~/.fluvio/config`. For example:
```
version = "2.0"
current_profile = "mycube"
[profile.local]
cluster = "local"
[profile.mycube]
cluster = "mycube"
[cluster.local]
addr = "localhost:9003"
[cluster.mycube]
addr = "10.98.246.30:9003"
```

Profiles point to the current configuration. There can only be one active profile at a time. The `current_profile` field in the configuration file points to the current active profile.When the profile parameter is omitted, whatever is there in the `current_profile`, is used.
###Profile operations `fluvio profile -h`
* current-profile: Display the current context
```
$ fluvio profile current-profile
mycube
```

* create-local-profile: set profile to local servers
```
$ fluvio profile create-local-profile
local context is set to: localhost:9003
```

* switch-profile: There can be multiple profiles and you can switch among them.

`fluvio profile switch-profile <profile name>`
```
$ fluvio profile switch-profile local
$ fluvio profile current-profile
local
```

* delete-profile : Any selected profile can be  deleted.

`fluvio profile delete-profile <profile name>`

```
$ fluvio profile delete-profile local
profile local deleted
```
This gives the following warning
```
warning: this removed your current profile, use config switch-profile to select a different one
```

* create-k8-profile: set profile to kubernetes
To set the profile to kubernetes, fluvio service needs to be deployed. 

## Default Profile

Fluvio CLI has one **default** profile and an unlimited number of **user-defined** profiles. The **default** profile has the lowest precedence and it is looked-up in the following order:

* command line parameter **service** (&dash;&dash;sc, &dash;&dash;spu, &dash;&dash;kf),
* command line parameter **user-defined profile** (&dash;&dash;profile).
* **default profile**

The CLI searches for the **default.toml** profile file in the following order: 

* if $FLUVIO_HOME environment variable is set, look-up:
    ```bash
    $FLUVIO_HOME/.fluvio/profiles/default.toml
    ```
* if no environment variable is set, look-up:
    ```bash
    $HOME/.fluvio/profiles/default.toml 
    ```

->The directory hierarchy  **/.fluvio/profiles/** is preserved whether $FLUVIO_HOME is provisioned or not.


##Clusters
There are different clusters in fluvio, for instance local cluster. They can be either in the local machine or remote cloud.Cluster information in the config file gives the cluster information.

