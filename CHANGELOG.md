# Release Notes

## Platform Version 0.9.34 - UNRELEASED
* Added througput control to fluvio producer ([#2512](https://github.com/infinyon/fluvio/issues/2512))

## Platform Version 0.9.33 - 2022-08-10
* Added `DeliverySemantic` to `fluvio-cli`. ([#2508](https://github.com/infinyon/fluvio/pull/2508))
* CLI: Added ability to delete multiple connectors, smart modules and topics with one command. ([#2427](https://github.com/infinyon/fluvio/issues/2427))
* Added `--use-k8-port-forwarding` option to `fluvio cluster start`. ([#2516](https://github.com/infinyon/fluvio/pull/2516))
* SmartModule package: add missing metadata ([#2532](https://github.com/infinyon/fluvio/pull/2532))
* Adds feedback and debug info to 'smart-module create' ([#2513](https://github.com/infinyon/fluvio/pull/2513))
* Prevent collisions between namespaces ([#2539](https://github.com/infinyon/fluvio/pull/2539))
* Added proxy support during packages installation ([#2535](https://github.com/infinyon/fluvio/pull/2535))

## Platform Version 0.9.32 - 2022-07-26
* Restrict usage of `--initial`, `--extra-params` and `--join-topic` in `fluvio consume`. Those options only should be accepted when using specific smartmodules. ([#2476](https://github.com/infinyon/fluvio/pull/2476))
* Rename `--smartmodule` option in `fluvio consume` to `--smart-module`. `--smartmodule is still an alias for backward compatibility. ([#2485](https://github.com/infinyon/fluvio/issues/2485))
* Measure latency for stats using macro. ([#2483](https://github.com/infinyon/fluvio/pull/2483))
* Keep serving incoming requests even if socket closed to write. ([#2484](https://github.com/infinyon/fluvio/pull/2484))
* Support async response in multiplexed socket. ([#2488](https://github.com/infinyon/fluvio/pull/2488))
* Drop write lock before async IO operations. ([#2490](https://github.com/infinyon/fluvio/pull/2490))
* Add `Clone` trait to `DefaultProduceRequest`. ([#2501](https://github.com/infinyon/fluvio/pull/2501))
* Add `AtMostOnce` and `AtLeastOnce` delivery semantics. ([#2503](https://github.com/infinyon/fluvio/pull/2503))

## Platform Version 0.9.31 - 2022-07-13
* Move stream publishers to connection-level context ([#2452](https://github.com/infinyon/fluvio/pull/2452))
* Prefer ExternalIP to InternalIP if configured in kubernetes ([#2448](https://github.com/infinyon/fluvio/pull/2448))
* Add `fluvio connector config <connector-name>`  ([#2464](https://github.com/infinyon/fluvio/pull/2464))
* Add performance counters to producer ([#2424](https://github.com/infinyon/fluvio/issues/2424))
* Upgrade to fluvio-future 0.4.0 ([#2470](https://github.com/infinyon/fluvio/pull/2470))
* Add support to detecting smartmodule type from WASM payload on SPU  ([#2457](https://github.com/infinyon/fluvio/issues/2457))
* Require `version` field in connector yaml. ([#2472](https://github.com/infinyon/fluvio/pull/2472))

## Platform Version 0.9.30 - 2022-06-29
* Improve CLI error output when log_dir isn't writable ([#2425](https://github.com/infinyon/fluvio/pull/2425))
* Fix issue in producer when sending more than one batch in a request ([#2443](//github.com/infinyon/fluvio/issues/2443))
* Fix bug in `last_partition_offset` update when handling smartmodules on SPU ([#2432](https://github.com/infinyon/fluvio/pull/2432))
* Re-allow string, dictionaries and lists as options to `parameters` section in connector yaml. ([#2446](https://github.com/infinyon/fluvio/issues/2446))

## Platform Version 0.9.29 - 2022-06-27
* Revert 0.9.28 updates to Connector yaml config ([#2436](https://github.com/infinyon/fluvio/pull/2436))

## Platform Version 0.9.28 - 2022-06-26
* Upgrade to Wasmtime 0.37 ([#2400](https://github.com/infinyon/fluvio/pull/2400))
* Allow Cluster diagnostics to continue even if profile doesn't exist  ([#2400](https://github.com/infinyon/fluvio/pull/2402))
* Add timeout when creating SPG ([#2364](https://github.com/infinyon/fluvio/issues/2411))
* Log fluvio version and git rev on client creation ([#2403](https://github.com/infinyon/fluvio/issues/2403))
* Display multi-word subcommand aliases in CLI help info ([#2033](https://github.com/infinyon/fluvio/issues/2033))
* Add filter-map support to SmartProducer ([#2418](https://github.com/infinyon/fluvio/issues/2418))
* Fix `wasi` functions binding relying on order ([#2428](https://github.com/infinyon/fluvio/pull/2428))
* Add top level `producer` and `consumer` entries to connector yaml configurations. ([#2426](https://github.com/infinyon/fluvio/issues/2426))
* Allow string, dictionaries and lists as options to `parameters` section in connector yaml. ([#2426](https://github.com/infinyon/fluvio/issues/2426))

## Platform Version 0.9.27 - 2022-05-25
* Support installing clusters on Google Kubernetes Engine ([#2364](https://github.com/infinyon/fluvio/issues/2364))
* Make Zig Install more reliable ([#2388](https://github.com/infinyon/fluvio/issues/2388s))
* Add path setting hint for fish shell in install script ([#2389](https://github.com/infinyon/fluvio/pull/2389))
* Fix typo in `change_listener` function in `fluvio_types` crate ([#2382](https://github.com/infinyon/fluvio/pull/2382))

## Platform Version 0.9.26 - 2022-05-10
* Increase default `STORAGE_MAX_BATCH_SIZE` ([#2342](https://github.com/infinyon/fluvio/issues/2342))

## Platform Version 0.9.25 - 2022-05-04
* Set timestamp in Records while producing. ([#2288](https://github.com/infinyon/fluvio/issues/2288))
* Support `ReadCommitted` isolation in SPU for Produce requests [#2336](https://github.com/infinyon/fluvio/pull/2336)
* Improve error messages and add `--fix` option to `fluvio cluster check` to autofix recoverable errors ([#2308](https://github.com/infinyon/fluvio/issues/2308))
* Producer must respect ReadCommitted isolation [#2302](https://github.com/infinyon/fluvio/issues/2302)
* Add `{{time}}` option to `--format` in `fluvio consume` to display record timestamp ([#2345](https://github.com/infinyon/fluvio/issues/2345))

## Platform Version 0.9.24 - 2022-04-21
* CLI: Migrate all fluvio crates to `comfy-table` from `prettytable-rs` ([#2285](https://github.com/infinyon/fluvio/issues/2263))
* Storage: Enforce size based retention for topic ([#2179](https://github.com/infinyon/fluvio/issues/2179))
* Don't try to use directories as smartmodule if passed as argument ([#2292](https://github.com/infinyon/fluvio/issues/2292))
* CLI: Profile export ([#2327](https://github.com/infinyon/fluvio/issues/2327))

## Platform Version 0.9.23 - 2022-04-13
* Add `TYPE` column to `fluvio connector list` ([#2218](https://github.com/infinyon/fluvio/issues/2218))
* Use `Clap` instead of `StructOpt` for all CLI ([#2166](https://github.com/infinyon/fluvio/issues/2166))
* Re-enable ZSH completions ([#2283](https://github.com/infinyon/fluvio/issues/2283))
* Disable versions from displaying in CLI subcommands ([#1805](https://github.com/infinyon/fluvio/issues/1805))
* Increase default `MAX_FETCH_BYTES` in fluvio client ([#2259](https://github.com/infinyon/fluvio/issues/2259))
* Add `fluvio-channel` to `fluvio update` process ([#2221](https://github.com/infinyon/fluvio/issues/2221))

## Platform Version 0.9.22 - 2022-03-25
* Add topic level compression configuration ([#2249](https://github.com/infinyon/fluvio/issues/2249))
* Add producer batch related options for `fluvio produce`([#2257](https://github.com/infinyon/fluvio/issues/2257))

## Platform Version 0.9.21 - 2022-03-14
* Make store time out configurable ([#2212](https://github.com/infinyon/fluvio/issues/2212))
* Add a `size` field in the `fluvio partition list` output. This field represents the size of logs in the partition. ([#2148](https://github.com/infinyon/fluvio/issues/2148))
* Add support for partial CA Intermediate Trust Anchors ([#2232](https://github.com/infinyon/fluvio/pull/2232))
* Fix Installer problem with self-signed certs ([#2216](https://github.com/infinyon/fluvio/issues/2216))
* Report SPU error codes to FutureRecordMetadata ([#2228](https://github.com/infinyon/fluvio/issues/2228))
* Optimize partition size computation ([#2230](https://github.com/infinyon/fluvio/issues/2230))
* Fix fluvio-test configuration to support data generator ([#2237](https://github.com/infinyon/fluvio/pull/2237))
* Add compression support. ([#2082](https://github.com/infinyon/fluvio/issues/2082))

## Platform Version 0.9.20 - 2022-02-17
* Add `connector update -c config` to update the running configuration of a given existing managed connector ([#2188](https://github.com/infinyon/fluvio/pull/2188))
* Handle large number of produce and consumers ([#2116](https://github.com/infinyon/fluvio/issues/2116))
* Disable `fluvio update` when using pinned version channel ([#2155](https://github.com/infinyon/fluvio/issues/2155))
* Deprecate redundant `create_topic` flag from the connectors configuration ([#2200](https://github.com/infinyon/fluvio/issues/2200))

## Platform Version 0.9.19 - 2022-02-10
* Add WASI support to SmartEngine ([#1874](https://github.com/infinyon/fluvio/issues/1874))
* Fix incorrect behavior when consuming with a given offset in a partition with batches with more than one record. ([#2002](https://github.com/infinyon/fluvio/issues/2002))
* Add `version` column to `fluvio connector list` ([#2145](https://github.com/infinyon/fluvio/issues/2145))
* Reimport metadata for tableformat in fluvio client ([#2175](https://github.com/infinyon/fluvio/issues/2175))
* Change log level for admin actions to info in SC public services ([#2177](https://github.com/infinyon/fluvio/issues/2177))
* Fix problem with zero copy ([#2181](https://github.com/infinyon/fluvio/issues/2181))

## Platform Version 0.9.18 - 2022-01-31
* Show Platform version for fluvio run ([#2104](https://github.com/infinyon/fluvio/issues/2104))
* Fix batching producer for WASM platforms ([#2120](https://github.com/infinyon/fluvio/issues/2120))
* Make Test runner more reliable ([#2110](https://github.com/infinyon/fluvio/issues/2110))
* Fix connector crd to store version ([#2123](https://github.com/infinyon/fluvio/issues/2123))
* Remove max version requirements ([#2106](https://github.com/infinyon/fluvio/issues/2106))
* Optimize memory allocation  ([#2069](https://github.com/infinyon/fluvio/pull/2133))
* Translate `_` with `-` in connector parameters ([#2149](https://github.com/infinyon/fluvio/issues/2149))

## Platform Version 0.9.17 - 2022-01-12
* Change default values of TopicProducerConfig ([#2069](https://github.com/infinyon/fluvio/issues/2069))
* Enhance CLI Diagnostics with system info  ([#2069](https://github.com/infinyon/fluvio/pull/2092))
* Don't HTML escape output from CLI consumer using `--format` ([#1628](https://github.com/infinyon/fluvio/issues/1628))

## Platform Version 0.9.16 - 2022-01-03
* Consume with end ([#1940](https://github.com/infinyon/fluvio/issues/1940))
* Return base offset in partition produce response ([#2025](https://github.com/infinyon/fluvio/issues/2025))
* Simple Topic Retention with time ([#2019](https://github.com/infinyon/fluvio/issues/2019))
* Uninstall sys chart when cluster is deleted ([#2032](https://github.com/infinyon/fluvio/issues/2032))
* Upgrade Wasmtime 0.32 ([#2038](https://github.com/infinyon/fluvio/pull/2038))
* Add auto-batching to Producer ([#2000](https://github.com/infinyon/fluvio/issues/2000))
* Add support for 3rd party connectors ([#2027](https://github.com/infinyon/fluvio/pull/2027))
* Introduce channels into CLI ([#2021](https://github.com/infinyon/fluvio/issues/2021))
* Upgrade to Zig 0.9 and LLVM 13 ([#2046](https://github.com/infinyon/fluvio/pull/2046))
* Add API to converting to list type from metadata ([#2052](https://github.com/infinyon/fluvio/pull/2052))
* Check if the local cluster exists during installation ([#2041](https://github.com/infinyon/fluvio/issues/2041))

> Note:
> For existing CLI users, we recommend following [the instructions to re-install their CLI](https://www.fluvio.io/download/). This is a requirement in order to use Fluvio CLI Channels.

## Platform Version 0.9.15 - 2021-12-10
* Migrate Rust crates to edition 2021 ([#1798](https://github.com/infinyon/fluvio/issues/1798))
* TableFormat support for JSON array of objects ([#1967](https://github.com/infinyon/fluvio/issues/1967))

## Platform Version 0.9.14 - 2021-12-04
* Add support for tuple structs in fluvio-protocol derived macros. ([#1828](https://github.com/infinyon/fluvio/issues/1828))
* Expose fluvio completions in the top-level subcommand. ([#1850](https://github.com/infinyon/fluvio/issues/1850))
* Make installation more reliable ([#1961](https://github.com/infinyon/fluvio/pull/1961))
* Add Spinner to `fluvio consume` command. ([#1881](https://github.com/infinyon/fluvio/issues/1881))
* Change CLI multi-word subcommand names to use kebab-case. ([#1947](https://github.com/infinyon/fluvio/issues/1947)
* Update subcommand descriptions for consistency [#1948](https://github.com/infinyon/fluvio/issues/1948))
* Add `fluvio connector logs <connector name>` ([#1969](https://github.com/infinyon/fluvio/pull/1969)).

## Platform Version 0.9.13 - 2021-11-19
* Fix connector create with `create_topic` option to succeed if topic already exists. ([#1823](https://github.com/infinyon/fluvio/pull/1823))
* Add `#[smartstream(filter_map)]` for filtering and transforming at the same time. ([#1826](https://github.com/infinyon/fluvio/issues/1826))
* Add table display output option to consumer for json objects ([#1642](https://github.com/infinyon/fluvio/issues/1642))
* Streamlined Admin API ([#1803](https://github.com/infinyon/fluvio/issues/1803))
* Add SpuDirectory trait to Fluvio Client ([#1863](https://github.com/infinyon/fluvio/issues/1863))
* Add `fluvio consume <topic> --output=full_table` to render row updates over fullscreen terminal screen ([#1846](https://github.com/infinyon/fluvio/issues/1846))
* Fix macOS kubernetes cluster management ([#1867](https://github.com/infinyon/fluvio/pull/1867))
* Persist aggregate accumulator across file batches  ([#1869](https://github.com/infinyon/fluvio/pull/1869))
* Make Fluvio cluster working on Apple Silicon ([#1896](https://github.com/infinyon/fluvio/pull/1896))
* Rename `fluvio table` to `fluvio tableformat` ([#1918](https://github.com/infinyon/fluvio/pull/1918))
* Restrict max version in fluvio client ([#1930](https://github.com/infinyon/fluvio/issues/1930))
* Use version from the client in SmartEngine to encode/decode input/output ([#1924](https://github.com/infinyon/fluvio/pull/1924))

## Platform Version 0.9.12 - 2021-10-27
* Add examples for ArrayMap. ([#1804](https://github.com/infinyon/fluvio/issues/1804))
* Report error when missing #[1462] attribute in SmartStream ([#1462](https://github.com/infinyon/fluvio/issues/1462))
* Fix consumer read after restart #[1815] attribute in SmartStream ([#1815](https://github.com/infinyon/fluvio/issues/1815))
* Added smartengine feature flag to fluvio client which adds ability to apply smartstream on producer. ([#1788](https://github.com/infinyon/fluvio/pull/1788))

## Platform Version 0.9.11 - 2021-10-22
* Reconnect producer to SPU if network error. ([#770](https://github.com/infinyon/fluvio/issues/770))
* Merge fluvio-protocol-{api,core,codec} crates into fluvio-protocol ([#1594](https://github.com/infinyon/fluvio/issues/1594))
* Add `#[smarstream(array_map)]` for expanding one record into many ([#1335](https://github.com/infinyon/fluvio/issues/1335))
* Add capability to use input parameters in smartstreams ([#1643](https://github.com/infinyon/fluvio/issues/1643))
* Make it easier to debug inline chart ([#1779](https://github.com/infinyon/fluvio/issues/1779))

## Platform Version 0.9.10 - 2021-10-07
* Improve error handling for socket timeout ([#791](https://github.com/infinyon/fluvio/issues/791))
* Report error when using invalid WASM in SmartStream consumer ([#1713](https://github.com/infinyon/fluvio/pull/1713))
* Reduce time for validating log file([#1726](https://github.com/infinyon/fluvio/pull/1726))

## Platform Version 0.9.9 - 2021-09-30
* Add `impl std::error::Error for ErrorCode` for better error reporting ([#1693](https://github.com/infinyon/fluvio/pull/1693))
* Add ability to create a consumer that can read from multiple partitions concurrently.  ([#1568](https://github.com/infinyon/fluvio/issues/1568))
* Expose partition for `fluvio consume --format`. ([#1701](https://github.com/infinyon/fluvio/issues/1701))
* Fix consumer stream hanging after rollver ([#1700](https://github.com/infinyon/fluvio/issues/1700))
* Added deployment controller for managed connectors ([#1499](https://github.com/infinyon/fluvio/pull/1499)).

## Platform Version 0.9.8 - 2021-09-23
* Add progress indicator to `fluvio cluster start` ([#1627](https://github.com/infinyon/fluvio/pull/1627))
* Added `fluvio cluster diagnostics` to help debugging with support ([#1671](https://github.com/infinyon/fluvio/pull/1671))
* Fix installation of sys charts when running `fluvio cluster start --local --develop` ([#1647](https://github.com/infinyon/fluvio/issues/1647))

## Platform Version 0.9.7 - 2021-09-16
* Improve progress message in `fluvio cluster start --local` ([#1586](https://github.com/infinyon/fluvio/pull/1586))
* Fix handling large stream fetch ([#1630](https://github.com/infinyon/fluvio/pull/1630))
* Create error variant and propagate that in case of attempt of creation of a topic with an invalid name. ([#1464](https://github.com/infinyon/fluvio/issues/1464))

## Platform Version 0.9.6 - 2021-09-11
* Improve display representation for some variants in FluvioError type ([#1581](https://github.com/infinyon/fluvio/issues/1581))
* Add spinner to `fluvio cluster --local --setup` command ([#1574](https://github.com/infinyon/fluvio/pull/1574))
* Add `--format` string for custom Consumer printouts ([#1593](https://github.com/infinyon/fluvio/issues/1593))

## Platform Version 0.9.5 - 2021-09-02
* Update `Debug` printout for `SmartStreamWasm` to reduce noise ([#1524](https://github.com/infinyon/fluvio/pull/1524))
* Increase platform stability ([#1497](https://github.com/infinyon/fluvio/pull/1497))
* Spawn a thread to handle stream fetch requests ([#1522](https://github.com/infinyon/fluvio/issues/1522))

## Platform Version 0.9.4 - 2021-08-26
* Publish docker image for aarch64 #1389 ([#1389](https://github.com/infinyon/fluvio/pull/1389))
* Do not panic when trying to create topic with space in the name. ([#1448](https://github.com/infinyon/fluvio/pull/1448))
* Deprecate consumer fetch API ([#957](https://github.com/infinyon/fluvio/issues/957))
* Gracefully handle error when trying to install plugins or update. ([#1434](https://github.com/infinyon/fluvio/pull/1434))
* Fix timing issue in Multiplexor Socket ([#1484](https://github.com/infinyon/fluvio/pull/1484))
* Compress WASM binaries. ([#1468](https://github.com/infinyon/fluvio/pull/1468))

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
* Socket close events propagate to client ([infinyon/fluvio-socket#22](https://github.com/infinyon/fluvio-socket/pull/22))
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
* Support Raspberry Pi
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
