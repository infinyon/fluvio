# Release Notes

## Platform Version 0.9.4 - UNRELEASED 
* Publish docker image for aarch64 #1389 ([#1389](https://github.com/infinyon/fluvio/pull/1389))
* Do not panic when trying to create topic with space in the name. ([#1448](https://github.com/infinyon/fluvio/pull/1448))
* Deprecate consumer fetch API ([#957](https://github.com/infinyon/fluvio/issues/957))
* Gracefully handle error when trying to install plugins or update. ([#1434](https://github.com/infinyon/fluvio/pull/1434))
* Fix timing issue in Multiplexor Socket ([#1484](https://github.com/infinyon/fluvio/pull/1484))

## Platform Version 0.9.3 - 2021-08-19
* Fix Replication timing. ([#1439](https://github.com/infinyon/fluvio/pull/1439))
* Fix release workflow to include Helm charts ([#1361](https://github.com/infinyon/fluvio/issues/1361))
* Implement SmartStream Aggregates (`#[smartstream(aggregate)]`) API ([#1173](https://github.com/infinyon/fluvio/issues/1173))
* Fix bounds when resolving relative Offsets ([#1210](https://github.com/infinyon/fluvio/issues/1210))
* Add `--tail` CLI option for reading recent records from a stream ([#1216](https://github.com/infinyon/fluvio/issues/1210))
* Fix consumer stream API that waited for records before initializing ([#986](https://github.com/infinyon/fluvio/issues/986))
* Fixed `fluvio install` for windows CLI ([#1461](https://github.com/infinyon/fluvio/pull/1461))

## Platform Version 0.9.2 - 2021-08-10
* Make Cluster installation more reliable. ([#1395](https://github.com/infinyon/fluvio/pull/1395))
* Reliabiility improvement in SC's SPU controller. ([#1413](https://github.com/infinyon/fluvio/pull/1413))

## Platform Version 0.9.1 - 2021-08-06
* Add Apple M1 as Tier2 platform for `fluvio-run` ([#1382](https://github.com/infinyon/fluvio/pull/1382))

## Platform Version 0.9.0 - 2021-08-03
* Add k8s feature flag to cli. ([#1257](https://github.com/infinyon/fluvio/pull/1257))
* Add windows build of cli and client. ([#1218](https://github.com/infinyon/fluvio/pull/1218))
* Improve `#[derive(Encoder, Decoder)]` to work with data enums. ([#1232](https://github.com/infinyon/fluvio/pull/1232))
* Fix Replication bug in K8 ([#1290](https://github.com/infinyon/fluvio/pull/1290))
* Add apply method to `StoreContext`. ([#1289](https://github.com/infinyon/fluvio/pull/1289))
* Build M1 mac CLI ([#132](https://github.com/infinyon/fluvio/pull/1312))
* Use inline helm chart ([#1292](https://github.com/infinyon/fluvio/pull/1292))
* Update `ConsumerConfig` with more idiomatic builder ([#1271](https://github.com/infinyon/fluvio/issues/1271))
* Improve `install.sh` to run on more targets ([#1269](https://github.com/infinyon/fluvio/issues/1269))
* Make `fluvio-cloud` an optional part of installation based on target support ([#1317](https://github.com/infinyon/fluvio/issues/1317))
* Remove `#[deprecated]` items from crates ([#1299](https://github.com/infinyon/fluvio/issues/1299))
* Bump `MINIMUM_PLATFORM_VERSION` to `0.9.0` ([#1310](https://github.com/infinyon/fluvio/issues/1310))
* Fix owner reference type to work delete in K 1.20.0 ([#1342](https://github.com/infinyon/fluvio/issues/1342))
* Fix Upgrading K8 Cluster ([#1347](https://github.com/infinyon/fluvio/issues/1347))
* Add Error Handling to SmartStreams ([#1198](https://github.com/infinyon/fluvio/pull/1198))
* Finish SmartStream Map (`#[smartstream(map)]`) API ([#1174](https://github.com/infinyon/fluvio/pull/1174), [#1198](https://github.com/infinyon/fluvio/pull/1198))

## Platform Version 0.8.5 - 2021-07-14
* Add unstable Admin Watch API for topics, partitions, and SPUs ([#1136](https://github.com/infinyon/fluvio/pull/1136))
* Make recipes for smoke tests no longer build by default, helps caching. ([#1165](https://github.com/infinyon/fluvio/pull/1165))
* Relax requirement of `FluvioAdmin` methods from `&mut self` to `&self`. ([#1178](https://github.com/infinyon/fluvio/pull/1178))
* Sort output of `fluvio partition list` by Topic then Partition. ([#1181](https://github.com/infinyon/fluvio/issues/1181))
* Add SmartStream Map (`#[smartstream(map)]`) API for transforming records. ([#1174](https://github.com/infinyon/fluvio/pull/1174))
* Change C compiler to `zig` and linker to `lld`. Resolves segfaults when cross compiling to musl. ([#464](https://github.com/infinyon/fluvio/pull/464))
* Consumer CLI prints a status when consuming from the end of a partition. ([#1171](https://github.com/infinyon/fluvio/pull/1171))
* Upgrade wasmtime to thread-safe API. ([#1200](https://github.com/infinyon/fluvio/issues/1200))
* Update fluvio-package to support arbitrary Targets. ([#1234](https://github.com/infinyon/fluvio/pull/1234))
* Future-proof PackageKind by deserializing all Strings. ([#1249](https://github.com/infinyon/fluvio/pull/1249))

## Platform Version 0.8.4 - 2021-05-29
* Don't hang when check for non exist topic. ([#697](https://github.com/infinyon/fluvio/pull/697))
* `fluvio cluster start` uses Kubernetes NodePort by default ([#1083](https://github.com/infinyon/fluvio/pull/1083))
* Use OpenSSL for Client ([#1150](https://github.com/infinyon/fluvio/pull/1150))
* Add `--raw` flag to `fluvio produce` for sending whole file input ([#1149](https://github.com/infinyon/fluvio/pull/1148))

## Platform Version 0.8.3 - 2021-05-25
* Added builder for fluvio_storage::config::ConfigOption. ([#1076](https://github.com/infinyon/fluvio/pull/1076))
* Use batch record sending in CLI producer ([#915](https://github.com/infinyon/fluvio/issues/915))
* Now ResponseApi and RequestApi encoder-decoders are symmetric ([#1075](https://github.com/infinyon/fluvio/issues/1075))
* `FluvioCodec` encoder now supports `FluvioEncoder` types. Implementation with bytes::Bytes now is deprecated. ([#1076](https://github.com/infinyon/fluvio/pull/1081))
* Added implementations of FluvioEncoder for &T: FluvioEncoder. ([#1081](https://github.com/infinyon/fluvio/pull/1081))
* Updated RecordAPI with RecordKey and RecordData ([#1088](https://github.com/infinyon/fluvio/issues/1088))
* Support WASM for client ([#1101](https://github.com/infinyon/fluvio/issues/1101))
* `spu_pool` to support wasm runtime. ([#1106](https://github.com/infinyon/fluvio/pull/1106))
* Remove async trait for more wasm support to client ([#1108](https://github.com/infinyon/fluvio/pull/1108))
* Better logging for SPU health check ([#1109](https://github.com/infinyon/fluvio/issues/1109))
* fluvio-socket build for wasm32 ([#1111](https://github.com/infinyon/fluvio/issues/1111))
* Add Fluvio::connect_with_connector to support custom connectors. ([#1120](https://github.com/infinyon/fluvio/issues/1120))

## Platform Version 0.8.2 - 2021-05-06
* Fix Replication fail over with duplication ([#1052](https://github.com/infinyon/fluvio/pull/1052))
* Relax platform version requirement for upgrade check ([#1055](https://github.com/infinyon/fluvio/pull/1055))
* Update logic for finding latest package release ([#1061](https://github.com/infinyon/fluvio/pull/1061))

## Platform Version 0.8.1 - 2021-05-03
* Use file name for the external commands (fixes #889) ([#1008](https://github.com/infinyon/fluvio/pull/1008))
* Fix Fluvio log directory on K8 ([#1043](https://github.com/infinyon/fluvio/pull/1043))
* Add RecordKey API for sending records without keys ([#985](https://github.com/infinyon/fluvio/pull/985))
* Make Fluvio Client compatitble with WASM ([#1042](https://github.com/infinyon/fluvio/pull/1042))
* Update Replication logic for SPU ([#1011](https://github.com/infinyon/fluvio/pull/1011))

## Platform Version 0.8.0 - 2021-04-27
* Added Partitioner trait for assigning partitions based on record keys ([#965](https://github.com/infinyon/fluvio/pull/965))
* Deprecated the `TopicProducer::send_record` method ([#965](https://github.com/infinyon/fluvio/pull/965))
* Decoupled Fluvio CLI from Fluvio server components ([#928](https://github.com/infinyon/fluvio/pull/928))

## Platform Version 0.7.3 - 2021-04-02
* Added batching for producing records with `send_all` API ([#896](https://github.com/infinyon/fluvio/pull/896)).
* WASM based Smart Stream Filter MVP ([#901](https://github.com/infinyon/fluvio/pull/901)).
* Fix topic not being deleted when SPU goes offline ([#914](https://github.com/infinyon/fluvio/pull/914))

## Platform Version 0.7.2 - 2021-03-23
* `fluvio update` updates plugins as well as CLI ([#865](https://github.com/infinyon/fluvio/issues/865)).
* SPU controller uses SVC ingress annotation ([#888](https://github.com/infinyon/fluvio/pull/888)).

## Platform Version 0.7.1 - 2021-03-15
* Client Key/Value support for producers and consumers
([#828](https://github.com/infinyon/fluvio/pull/828)).
* CLI Key/Value interface ([#830](https://github.com/infinyon/fluvio/pull/830))
* CI Reliability updates ([#842](https://github.com/infinyon/fluvio/pull/842)),
([#832](https://github.com/infinyon/fluvio/pull/832))

## Platform Version 0.7.0 - 2021-02-24
* `fluvio cluster upgrade` ([#709](https://github.com/infinyon/fluvio/pull/709))
* `install.sh` script works with `VERSION=latest` for prereleases([#812](https://github.com/infinyon/fluvio/pull/812))
* Fix stream fetch ([#769](https://github.com/infinyon/fluvio/pull/769))
* Limit for batchsize ([#787](https://github.com/infinyon/fluvio/pull/787))
* Protocol updates ([#752](https://github.com/infinyon/fluvio/pull/752))
* Socket close events propigate to client ([infinyon/fluvio-socket#22](https://github.com/infinyon/fluvio-socket/pull/22))
* Fix sha256 has in `fluvio version` ([#740](https://github.com/infinyon/fluvio/pull/740))
* Implement flush policy to flush on a delay time after writes ([#694](https://github.com/infinyon/fluvio/pull/694))
* Create basedir during `fluvio install` if missing ([#739](https://github.com/infinyon/fluvio/pull/739))

## Client 0.5.0
* Protocol changes to encode vector lengths in `DefaultAsyncBuffer` and `RecordSets` ([#752](https://github.com/infinyon/fluvio/pull/752)).

## Client 0.4.0
* Added `bytes` and `bytes_ref` for `Record` and removing `try_into_bytes` ([#706](https://github.com/infinyon/fluvio/pull/706))

## Platform Version 0.6.1 - 2020-1-16

## Bug Fixes
* Restore Ok ([#675](https://github.com/infinyon/fluvio/pull/675))

## Client
* Expose Consumer Record ([#687](https://github.com/infinyon/fluvio/pull/687))

## Installer
* Ability to customize chart using helm values ([688](https://github.com/infinyon/fluvio/pull/688))
* Disable SPU check ([686](https://github.com/infinyon/fluvio/pull/686))


## Version 0.6.0 - 2020-01-11

## New Features

### CLI
* Stand alone Installer
* Support for Writing Extension
* Self Update Capability
* Profile rename
* Show Platform versions

## Client
* API Docs
* Stream based Fetch

## Platform
* Cluster Installer with API and CLI support
* Support for Installing in Minikube without workaround
* Delete Topic
* Pluggable Authorization Framework with simple RBAC
* TLS endpoint for SC and SPU
* Connection multiplexing
* Support Rasberry Pi
* Use tracing instead of logger
* Github Action to test Platform
* K8: Enforce resource limit on Kubernetes

## Improvements

### CLI
* Consolidate Custom SPU and SPU
* Better error messages
* Move TLS as global option

### Client
* Fully multi-threaded consumer and producer

### Platform
* Better stability
* K8: Fix storage location
