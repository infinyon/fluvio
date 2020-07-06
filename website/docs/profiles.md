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
