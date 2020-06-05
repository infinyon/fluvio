---
title: Fluvio Profiles
menu: Profiles
weight: 20
---

Fluvio __profiles__ makes managing multiple deployments simple. A __profile__ is a .toml configuration file that stores the location of the services. The syntax is as follows:

{{< code lang="toml" style="light" >}}
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
{{< /code >}}

The parameters are as follows:

* __version__ is currently set to "1.0".
* __hostname/ip__ is the location of the service, it may be a domain name or an IP address.
* __port__ is the listening port of the service.

{{< caution >}}
While it is possible to configure all three services, it is not a useful configuration. Services with lower priority are shadowed by the services with higher priority. The lookup order is: SC => SPU => KF
{{< /caution >}}

The most common configuration is _one service per profile_.

{{< code lang="toml" style="light" >}}
version = "1.0"

[sc]
host = "sc.fluvio.dev.acme.com"
port = 9003
{{< /code >}}

#### Default Profile

Fluvio CLI has one __default__ profile and an unlimited number of __user-defined__ profiles. The __default__ profile has the lowest precedence and it is looked-up in the following order:

* command line parameter __service__ ({{< pre >}}--sc, --spu, --kf{{< /pre >}}),
* command line parameter __user-defined profile__ ({{< pre >}}--profile{{< /pre >}}).
* __default profile__

The CLI searches for the __default.toml__ profile file in the following order: 

* if $FLUVIO_HOME environment variable is set, look-up:
    {{< text >}}
    $FLUVIO_HOME/.fluvio/profiles/default.toml
    {{< /text >}}
* if no environment variable is set, look-up:
    {{< text >}}
    $HOME/.fluvio/profiles/default.toml 
    {{< /text >}}

{{< idea >}}
The directory hierarchy  __/.fluvio/profiles/__ is preserved whether $FLUVIO_HOME is provisioned or not.
{{< /idea >}}
