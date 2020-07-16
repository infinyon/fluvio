---
title: Fluvio Profiles
menu: Profiles
toc: true
weight: 20
---


There can be multiple types of clusters in fluvio.For example local cluster. The cluster can be either in the local machine or remote cloud.Cluster information in the config file gives the cluster information.
Fluvio **profiles** makes managing multiple deployments simple. A **profile** is a .toml configuration file that stores the location of the services. The syntax is as follows:

```bash
version = <profile-version>

current_profile = <profile_name>

[profile.<local_profile_name]
cluster = <local_profile_name>

[profile.<non_local_profile_name>]
cluster = <non_local_profile_name>

[cluster.local]
addr = <local_address>
[cluster.mycube]
addr = <non_local_address>

```

The parameters are as follows:

* **version** is currently set to "2.0".
* **current_profile** points to the current active profile.When the profile parameter is omitted, whatever is there in the `current_profile`, is used.
* **cluster** can be local or non-local clusters.
* **addr** gives the address of all the existing clusters.

The configuraton file is stored at `~/.fluvio/config`. For example:
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

Profiles point to the current configuration. There can only be one active profile at a time. The `current_profile` field in the configuration file points to the current active profile. When the profile parameter is omitted whatever is in the `current_profile` is used.

### Profile operations

Command line help is available at any level by appending -h or ‐‐help to the command. At top level, you can run fluvio with without arguments to get a list of available options.

```bash
Profile operation

fluvio profile <SUBCOMMAND>

FLAGS:
    -h, --help    Prints help information

SUBCOMMANDS:

    current-profile      Display the current context
    switch-profile          
    delete-profile          
    create-local-profile    set profile to local servers
    create-k8-profile       set profile to kubernetes cluster
    view                    Display entire configuration
    help        Prints this message or the help of the given subcommands
```

 * **current-profile** : displays the current context
    ```
    $ fluvio profile current-profile
    mycube
    ```

* **create-local-profile** : set profile to local servers
    ```
    $ fluvio profile create-local-profile
    local context is set to: localhost:9003
    ```

* **switch-profile** : transfers context to another profile

    ```
    $ fluvio profile switch-profile local
    $ fluvio profile current-profile
    local
    ```

* **delete-profile** : removes a profile from the client

    ```
    $ fluvio profile delete-profile local
    profile local deleted
    ```
    This gives the following warning

    ~> **Warning** : this removed your current profile, use config switch-profile to select a different one.


* **create-k8-profile** : set profile to kubernetes
To set the profile to kubernetes, fluvio service needs to be deployed. 
