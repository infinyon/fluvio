## Request

```
fluvio cluster status tells state of cluster:

none(discoverable)
stopped (no sc, spu running but data exits)
running (local/k8)
```
-------------------------------------------------------------------------------

# Existing Commands

### fluvio cluster

##### fluvio cluster start local

calls `fluvio-cluster/src/cli/start/mod.rs#StartOpt.process()`
    calls `fluvio-cluster/src/cli/start/local.rs#process_local()`
        calls `fluvio-cluster/src/cli/start/local.rs#install_local()`
            calls `fluvio-cluster/src/start/local.rs#LocalInstaller#install()`
                calls `fluvio-cluster/src/start/local.rs#LocalInstaller#launch_sc()`
                    calls `sc_process.start`
                    calls `fluvio-cluster/src/start/common.rs#try_connect_to_sc()` - returns Some(Fluvio) or None
                calls `fluvio-cluster/src/start/local.rs#LocalInstaller#launch_spu_group()`
                calls `fluvio-cluster/src/start/local.rs#LocalInstaller#confirm_spu()   `
                    creates a `FluvioAdmin` with `fluvio.admin()`
                    calls `admin.list::<SpuSpec, _>()` to get `spus`(`Vec<Metadata<SpuSpec>>`)
                    calls `filter` on `spus` using `spu.status.is_online()` 
                    Using that count to report fraction of spus online


##### fluvio cluster start (k8s)

calls `fluvio-cluster/src/cli/start/mod.rs#StartOpt.process()`
    calls `fluvio-cluster/src/cli/start/k8.rs#process_k8()`
        calls `fluvio-cluster/src/cli/start/k8.rs#start_k8()`
            calls `fluvio-cluster/src/start/k8.rs#install_fluvio()`
                calls `fluvio-cluster/src/start/k8.rs#install_app()`
                calls `fluvio-cluster/src/start/k8.rs#discover_sc_service()` to get `sc_service` (external address of SC)
                calls `fluvio-cluster/src/start/k8.rs#wait_for_sc_availability()` 
                calls `fluvio-cluster/src/start/k8.rs#discover_sc_external_host_and_port(&sc_service)` 
                rectifies `install_host_and_port` as `external_host_and_port` or `install_host and install_port` from `self.start_sc_port_forwarding`
                creates a `FluvioConfig` with `install_host_and_port`
                calls `fluvio-cluster/src/start/common.rs#try_connect_to_sc`
                saves `external_host_and_port` to `self.config.profile` or something using `self.update_profile`
                calls `fluvio-cluster/src/start/k8.rs#create_managed_spu_group()`
                    checks if spu group already exists using `admin.list::<SpuGroupSpec>` 
                
 

##### fluvio cluster spu list

List all SPUs known by this cluster (managed AND custom)

creates an `FluvioAdmin` with `fluvio.admin()`,
    calls `admin.list::<SpuSpec, _>()`
        creates an `ObjectApiListRequest`
            gets a response by calling `self.send_receive(list_request).await?;`
                


-------------------------------------------------------------------------------

# Implementing the Requested Command

### None
```
/// No Stream Controller or Stream Processing Units running and there is
/// no data in the cluster?
none(discoverable)
```

### Stopped
```
/// No Stream Controller or Stream Processing Units connected/running
/// but there is 'data' in this cluster?
stopped (no sc, spu running but data exits)
```

### Running
```
There is a Stream Controller running or connected?
running (local/k8)
```

### Psuedocode

```
async fn status() {
    if sc_running() and spus_running() {
        return "running".to_string()
    } else {
        if cluster_has_data() {
            return "stopped".to_string()
        } else {
            return "none".to_string()
        }
    }
}
```
