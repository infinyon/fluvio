# Release Notes

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## Platform Version 0.11.10-dev-2 - 2024-07-18

### Added

* Generate command params ([#4026](https://github.com/infinyon/fluvio/issues/4026))
* Add partition prd ([#4075](https://github.com/infinyon/fluvio/issues/4075))
* Add update commands for public api requests ([#4076](https://github.com/infinyon/fluvio/issues/4076))
* Add partitions ([#4065](https://github.com/infinyon/fluvio/issues/4065))
* Add mirror remote ([#4097](https://github.com/infinyon/fluvio/issues/4097))

### CI

* `0.11.9` post-release ([#4060](https://github.com/infinyon/fluvio/issues/4060))
* Update release checklist ([#4010](https://github.com/infinyon/fluvio/issues/4010))
* Default to zigbuild for cross-compilation ([#4103](https://github.com/infinyon/fluvio/issues/4103))

### Changed

* Update to Rust `1.79.0` ([#4064](https://github.com/infinyon/fluvio/issues/4064))
* Update `DEVELOPER.md` ([#4070](https://github.com/infinyon/fluvio/issues/4070))
* Update bytes crate ([#4087](https://github.com/infinyon/fluvio/issues/4087))

### Fixed

* Provides context on fvm tests ([#4062](https://github.com/infinyon/fluvio/issues/4062))
* Post-pone group ownership check on `generate` ([#4001](https://github.com/infinyon/fluvio/issues/4001))
* Mirror apply description ([#4078](https://github.com/infinyon/fluvio/issues/4078))
* Fix add partition not found ([#4080](https://github.com/infinyon/fluvio/issues/4080))
* Spu connections on kubernetes ([#4083](https://github.com/infinyon/fluvio/issues/4083))
* Delete topic partitions ([#4094](https://github.com/infinyon/fluvio/issues/4094))
* Produce recognize new partitions ([#4090](https://github.com/infinyon/fluvio/issues/4090))
* Load replica size ([#4104](https://github.com/infinyon/fluvio/issues/4104))

### Other

* Use `std::io::IsTerminal` over `atty` ([#4074](https://github.com/infinyon/fluvio/issues/4074))
* Access local metadata without k8 flag ([#4092](https://github.com/infinyon/fluvio/issues/4092))

## Platform Version 0.11.9 - 2024-06-07

### Added

* SNI Prefix on SPU to SPU ([#4056](https://github.com/infinyon/fluvio/pull/4056))
* SDF Publish Support ([#4053](https://github.com/infinyon/fluvio/issues/4053))
* Version checker for Resume ([#3999](https://github.com/infinyon/fluvio/issues/3999))
* Support to update artifacts in current fluvio release ([#4013](https://github.com/infinyon/fluvio/issues/4013))
* Add fluvio-compression as required dep ([#4012](https://github.com/infinyon/fluvio/issues/4012))
* Export mirror file with tls ([#4016](https://github.com/infinyon/fluvio/issues/4016))
* Tls and authorization on SPU mirroring ([#4022](https://github.com/infinyon/fluvio/issues/4022))
* Add sc opts to cluster start ([#4033](https://github.com/infinyon/fluvio/issues/4033))
* `fvm self update` support ([#4020](https://github.com/infinyon/fluvio/issues/4020))
* Add consumer --mirror argument to consume only from the selected ([#4048](https://github.com/infinyon/fluvio/issues/4048))
* Prompt when deleting cluster ([#4034](https://github.com/infinyon/fluvio/issues/4034))

### CI

* Publish support for readme ([#4032](https://github.com/infinyon/fluvio/issues/4032))

### Changed

* Expose types needed to construct connector config ([#4011](https://github.com/infinyon/fluvio/issues/4011))
* Update time to 0.3.36 ([#4023](https://github.com/infinyon/fluvio/issues/4023))
* Update dep cargo-generate ([#4018](https://github.com/infinyon/fluvio/issues/4018))

### Fixed

* Action urn implemented as individual objects ([#4024](https://github.com/infinyon/fluvio/issues/4024))
* Add mirroring e2e test ([#4028](https://github.com/infinyon/fluvio/issues/4028))
* Disallow produce mirror topic from home ([#4029](https://github.com/infinyon/fluvio/issues/4029))
* Disallow delete topic from remote ([#4046](https://github.com/infinyon/fluvio/issues/4046))

### Other

* Tls and authorization on SC mirroring ([#4017](https://github.com/infinyon/fluvio/issues/4017))
* Instance level authorization for basic authorization ([#4021](https://github.com/infinyon/fluvio/issues/4021))
* Show remote and home cmds ([#4047](https://github.com/infinyon/fluvio/issues/4047))

### Removed

* Delete topic mirrors ([#4030](https://github.com/infinyon/fluvio/issues/4030))

## Platform Version 0.11.9-dev - 2024-05-12

### CI

* Post-release ([#3996](https://github.com/infinyon/fluvio/issues/3996))
* Update infinyon/regex-filter@0.2.0 for wasi ([#3997](https://github.com/infinyon/fluvio/issues/3997))
* Use smdk artifact for ci tests ([#4000](https://github.com/infinyon/fluvio/issues/4000))

### Fixed

* Config parsing ignores invalid `transforms` value ([#4004](https://github.com/infinyon/fluvio/issues/4004))
* `sink` generation for conns fails to build ([#4005](https://github.com/infinyon/fluvio/issues/4005))

## Platform Version 0.11.8 - 2024-05-07

### Added

* Spu-to-spu mirroring connection ([#3956](https://github.com/infinyon/fluvio/issues/3956))

### Changed

* `PackageSet::check_artifact_updates` method ([#3987](https://github.com/infinyon/fluvio/issues/3987))

### Fixed

* Explicitly enable wasi on smartengine uses ([#3988](https://github.com/infinyon/fluvio/issues/3988))

## Platform Version 0.11.7 - 2024-05-01

### Added
* Forbid `fluvio cluster start` when it should be resumed ([#3695](https://github.com/infinyon/fluvio/pull/3965))
* Default to wasi supported build arch ([#3981](https://github.com/infinyon/fluvio/issues/3981))
* Add cmd to create mirror topics ([#3962](https://github.com/infinyon/fluvio/issues/3962))

### Fixed

* Use fluvio_bin env instead fluvio directly ([#3960](https://github.com/infinyon/fluvio/issues/3960))
* Typos for `Pacakge` in documentation ([#3964](https://github.com/infinyon/fluvio/issues/3964))
* Upadate http client, better client isolation ([#3980](https://github.com/infinyon/fluvio/issues/3980))

### Other

* Enable `repository_url` ([#3961](https://github.com/infinyon/fluvio/issues/3961))
* Rename mirroring cmds to home and remote ([#3959](https://github.com/infinyon/fluvio/issues/3959))
* Rename SpuSocket to StreamSocket ([#3958](https://github.com/infinyon/fluvio/issues/3958))
* Rename remote, edge and core to mirror, remote and home ([#3966](https://github.com/infinyon/fluvio/issues/3966))
* Graceful period for stream interruption ([#3969](https://github.com/infinyon/fluvio/issues/3969))
* Sc-to-sc mirroring connection ([#3946](https://github.com/infinyon/fluvio/issues/3946))
* Allow enabling wasi for connectors ([#3977](https://github.com/infinyon/fluvio/issues/3977))
* Set default offset flush interval to 2s ([#3976](https://github.com/infinyon/fluvio/issues/3976))
* `v0.11.6` post-release ([#3955](https://github.com/infinyon/fluvio/issues/3955))

## Platform Version 0.11.6 - 2024-04-20

### Added

* Added fluvio kv storage crate ([#3905](https://github.com/infinyon/fluvio/issues/3905))
* Add docker compose example ([#3910](https://github.com/infinyon/fluvio/issues/3910))
* Added public SPU requests for offset mngt ([#3918](https://github.com/infinyon/fluvio/issues/3918))
* Add consumer storage to spu ([#3915](https://github.com/infinyon/fluvio/issues/3915))
* Added offset management to consumer api(unstable) ([#3928](https://github.com/infinyon/fluvio/issues/3928))
* Added hidden topics and partitions ([#3930](https://github.com/infinyon/fluvio/issues/3930))
* Added auto-creation of offsets topic ([#3935](https://github.com/infinyon/fluvio/issues/3935))
* Added consumer offset to cli ([#3941](https://github.com/infinyon/fluvio/issues/3941))
* Support subset of partitions in consumer ext ([#3948](https://github.com/infinyon/fluvio/issues/3948))
* Added consumer offset to connector config ([#3950](https://github.com/infinyon/fluvio/issues/3950))

### CI

* Ci, update fluvio publish workflows ([#3939](https://github.com/infinyon/fluvio/issues/3939))
* Release `v0.11.6

### Changed

* Update some dep ([#3929](https://github.com/infinyon/fluvio/issues/3929))

### Fixed

* Check if batch size exceeds segment size ([#3900](https://github.com/infinyon/fluvio/issues/3900))
* Improve kubectl access errors ([#3895](https://github.com/infinyon/fluvio/issues/3895))
* Use specialized error for evicted offset case ([#3902](https://github.com/infinyon/fluvio/issues/3902))
* Fvm switch bug #3765 ([#3912](https://github.com/infinyon/fluvio/issues/3912))
* Cargo_template Cargo.toml noise ([#3919](https://github.com/infinyon/fluvio/issues/3919))
* Cdk deploy, fix contention on ipkg binary and/or log path ([#3926](https://github.com/infinyon/fluvio/issues/3926))

### Other

* Enable minallocator to improve perf ([#3924](https://github.com/infinyon/fluvio/issues/3924))
* Avoid merging PR if a job was skipped/failed ([#3934](https://github.com/infinyon/fluvio/issues/3934))
* Disallow deletion system topic unless forced ([#3942](https://github.com/infinyon/fluvio/issues/3942))
* Mark consumers offset feature as stable ([#3945](https://github.com/infinyon/fluvio/issues/3945))
* Ci, bind latest to latest fluvio-cloud ([#3949](https://github.com/infinyon/fluvio/issues/3949))

### Removed

* Set old consumer API deprecated ([#3947](https://github.com/infinyon/fluvio/issues/3947))

## Platform Version 0.11.5 - 2024-03-04

### Added

* Add offset management proposal ([#3856](https://github.com/infinyon/fluvio/issues/3856))

### Changed

* When topic is deleted, connected consumers are notified [#3861](https://github.com/infinyon/fluvio/pull/3861)
* Update bug issue template ([#3864](https://github.com/infinyon/fluvio/issues/3864))
* Update deps, cargo-generate ([#3865](https://github.com/infinyon/fluvio/issues/3865))
* Run cargo update -p curve25519-dalek@4.1.1 ([#3867](https://github.com/infinyon/fluvio/issues/3867))
* Relax hub access login requirements ([#3850](https://github.com/infinyon/fluvio/issues/3850))

### Fixed

* `fluvio profile add` w/ no config file ([#3874](https://github.com/infinyon/fluvio/issues/3874))
* Smdk publish ([#3873](https://github.com/infinyon/fluvio/issues/3873))
* Fix version ([#3879](https://github.com/infinyon/fluvio/issues/3879))
* Update dep ed25519-dalek ([#3872](https://github.com/infinyon/fluvio/issues/3872))
* Update README.md (tiny typo fixed) ([#3849](https://github.com/infinyon/fluvio/issues/3849))
* Fvm should allow general pkgset labels ([#3854](https://github.com/infinyon/fluvio/issues/3854))
* Regenerate certificates ([#3841](https://github.com/infinyon/fluvio/issues/3841))

### CI

* Ci publish, fix misaligned sha ([#3855](https://github.com/infinyon/fluvio/issues/3855))
* Ci, publish fvm as an artifact ([#3871](https://github.com/infinyon/fluvio/issues/3871))
* Post-release 0.11.4 ([#3845](https://github.com/infinyon/fluvio/issues/3845))
* Ci, tag stable fluvio releases with stable in docker hub ([#3846](https://github.com/infinyon/fluvio/issues/3846))
* Ci stability improvements ([#3851](https://github.com/infinyon/fluvio/issues/3851))
* Ci, fix release ([#3844](https://github.com/infinyon/fluvio/issues/3844))

### Other

* Update VERSION to 0.11.5-dev-1 ([#3878](https://github.com/infinyon/fluvio/issues/3878))
* :white_check_mark: tests for cli when consuming with format ([#3885](https://github.com/infinyon/fluvio/issues/3885))
* Prerel 0.11.5-dev-2 ([#3883](https://github.com/infinyon/fluvio/issues/3883))
* List included artifacts with `fvm list <channel>` ([#3877](https://github.com/infinyon/fluvio/issues/3877))
* Exit when there is no unzip cmd ([#3852](https://github.com/infinyon/fluvio/issues/3852))
* Remove unnecessary specialized errors ([#3853](https://github.com/infinyon/fluvio/issues/3853))


## Platform Version 0.11.4 - 2024-01-27

### Added

* Small updates to admin::delete and consumer docs ([#3827](https://github.com/infinyon/fluvio/issues/3827))
* Add Encoder and Decoder impl to float rust types ([#3834](https://github.com/infinyon/fluvio/issues/3834))

### Changed

* Release fluvio v0.11.4 ([#3839](https://github.com/infinyon/fluvio/issues/3839))
* Update README.md ([#3835](https://github.com/infinyon/fluvio/issues/3835))

### Fixed

* Fluvio cluster start, improve message when cluster still running ([#3832](https://github.com/infinyon/fluvio/issues/3832))

### Removed

* Remove separate `cdk` installation ([#3833](https://github.com/infinyon/fluvio/issues/3833))

## Platform Version 0.11.3 - 2024-01-15

### Fixed
* Setup fluvio gh action ([#3775](https://github.com/infinyon/fluvio/issues/3775))
* Address some typos and grammar issues in README ([#3804](https://github.com/infinyon/fluvio/issues/3804))
* `FluvioVersionPrinter` support for json output ([#3807](https://github.com/infinyon/fluvio/issues/3807))

### CI
* Post release `v0.11.2` ([#3770](https://github.com/infinyon/fluvio/issues/3770))
* Ci, update outdated rust toolchain install ([#3771](https://github.com/infinyon/fluvio/issues/3771))
* Don't require git to build fluvio ([#3789](https://github.com/infinyon/fluvio/issues/3789))
* Ci, fixup release for pre-release, use stable fluvio cloud ver ([#3814](https://github.com/infinyon/fluvio/issues/3814))

### Changed

* Use `setup-fluvio` action ([#3779](https://github.com/infinyon/fluvio/issues/3779))
* Update rust 1.75 ([#3801](https://github.com/infinyon/fluvio/issues/3801))
* Update README.md ([#3802](https://github.com/infinyon/fluvio/issues/3802))

### Fixed

* Ci, fix hourly k8 test ([#3774](https://github.com/infinyon/fluvio/issues/3774))
* Use `INFINYON_HUB_REMOTE`  over `HUB_REGISTRY_URL` for FVM ([#3796](https://github.com/infinyon/fluvio/issues/3796))
* Default connector sink template compilation error ([#3797](https://github.com/infinyon/fluvio/issues/3797))
* Cd_dev fixup ([#3812](https://github.com/infinyon/fluvio/issues/3812))

### Other

* Fluvio-futures/http client integration ([#3761](https://github.com/infinyon/fluvio/issues/3761))
* Use ip in local profile ([#3791](https://github.com/infinyon/fluvio/issues/3791))
* Rename `transform` and `transforms-file` args ([#3792](https://github.com/infinyon/fluvio/issues/3792))
* Standardized version output with table format ([#3803](https://github.com/infinyon/fluvio/issues/3803))
* Export utility functions from version printing ([#3805](https://github.com/infinyon/fluvio/issues/3805))
* Use new version api on smdk and fluvio cli ([#3809](https://github.com/infinyon/fluvio/issues/3809))

### Removed

* Fluvio-cli-common, remove unused dep ([#3786](https://github.com/infinyon/fluvio/issues/3786))



## Platform Version 0.11.2 - 2023-12-08

### CI

* Use `ctx` ci for installations ([#3767](https://github.com/infinyon/fluvio/issues/3767))

## Platform Version 0.11.2-dev-1 - 2023-12-04

### Added

* Added minimal support for docker inst type ([#3755](https://github.com/infinyon/fluvio/issues/3755))
* Update README.md to show Fluvio Diagram ([#3757](https://github.com/infinyon/fluvio/issues/3757))
* FVM uninstall command ([#3756](https://github.com/infinyon/fluvio/issues/3756))

### Fixed

* Topic reducer should wait for partitions sync ([#3752](https://github.com/infinyon/fluvio/issues/3752))

## Platform Version 0.11.1 - 2023-11-24

### Added

* Support cluster upgrade/check/status for local cluster ([#3719](https://github.com/infinyon/fluvio/issues/3719))

### Changed

* Clone `smdk test` to `fluvio sm test` ([#3559](https://github.com/infinyon/fluvio/issues/3559))
* Hide `fvm self install` from and help introduce `fvm self update` ([#3724](https://github.com/infinyon/fluvio/issues/3724))
* Make local cluster as default in cli ([#3733](https://github.com/infinyon/fluvio/issues/3733))
* Use installation type in cli commands ([#3691](https://github.com/infinyon/fluvio/issues/3691))

### Fixed

* Parse `ApiError` from Hub and bubble up ([#3692](https://github.com/infinyon/fluvio/issues/3692))
* Macos remove binaries before replacing ([#3735](https://github.com/infinyon/fluvio/issues/3735))

### CI

* Put logs under $FLUVIO_HOME dir ([#3737](https://github.com/infinyon/fluvio/issues/3737))
* `0.11.0` post release ([#3711](https://github.com/infinyon/fluvio/issues/3711))
* Fvm_basic.bats, smoke test adjustment ([#3717](https://github.com/infinyon/fluvio/issues/3717))
* Update local clusters setup on CD and Hourly ([#3727](https://github.com/infinyon/fluvio/issues/3727))
* Explicitly set cluster type in CI/CD ([#3732](https://github.com/infinyon/fluvio/issues/3732))
* Remove implicit cluster start from tests ([#3726](https://github.com/infinyon/fluvio/issues/3726))
* Switch cli tests to local fluvio cluster ([#3720](https://github.com/infinyon/fluvio/issues/3720))

## Platform Version 0.11.0 - 2023-11-16

### Other

* Use a different config for tests updating the file ([#3708](https://github.com/infinyon/fluvio/issues/3708))

## Platform Version 0.11.1 - UNRELEASED

## Platform Version 0.11.0-dev-1 - 2023-11-15

### Added

* Add cascade deletion for local metadata ([#3638](https://github.com/infinyon/fluvio/issues/3638))
* Add support for cluster metadata ([#3628](https://github.com/infinyon/fluvio/issues/3628))

### CI

* Postrelease 0.10.17 ([#3637](https://github.com/infinyon/fluvio/issues/3637))
* Update wasmtime to v14.0.1 ([#3625](https://github.com/infinyon/fluvio/issues/3625))
* Fvm update command ([#3645](https://github.com/infinyon/fluvio/issues/3645))
* Add `cdk hub` command ([#3612](https://github.com/infinyon/fluvio/issues/3612))
* Disable fvm on ci ([#3660](https://github.com/infinyon/fluvio/issues/3660))
* Enable merge_group event in CI ([#3674](https://github.com/infinyon/fluvio/issues/3674))
* Ci, publish.yml remove branches ([#3690](https://github.com/infinyon/fluvio/issues/3690))
* Publish-pkgset, print inputs ([#3700](https://github.com/infinyon/fluvio/issues/3700))
* Release.mk, quotes cause pkgset publish to fail ([#3703](https://github.com/infinyon/fluvio/issues/3703))
* Ci, switch ci->publish->cd_dev to repo_dispatch ([#3705](https://github.com/infinyon/fluvio/issues/3705))

### Changed

* Update third party dep ([#3656](https://github.com/infinyon/fluvio/issues/3656))

### Fixed

* Sc should use namespace in k8 mode ([#3651](https://github.com/infinyon/fluvio/issues/3651))
* Use store version on metadata stream filter ([#3664](https://github.com/infinyon/fluvio/issues/3664))
* Check if custom spu exists on start ([#3668](https://github.com/infinyon/fluvio/issues/3668))
* Use `pkgset` over `version` ([#3672](https://github.com/infinyon/fluvio/issues/3672))
* Properly propagate sm errors on produce ([#3671](https://github.com/infinyon/fluvio/issues/3671))
* Misc fixes to failing tests in CI ([#3685](https://github.com/infinyon/fluvio/issues/3685))
* Ci fix cd_dev and cd_release ([#3687](https://github.com/infinyon/fluvio/issues/3687))
* Fvm rename fallback ([#3689](https://github.com/infinyon/fluvio/issues/3689))
* Skip already published instead of aborting ([#3695](https://github.com/infinyon/fluvio/issues/3695))
* Ci fix release.mk, publish-pkgset ([#3699](https://github.com/infinyon/fluvio/issues/3699))
* Ci, fix cd_dev upgrade-test.sh pull intended ver ([#3704](https://github.com/infinyon/fluvio/issues/3704))
* Ci, fix publish->cd_dev perms for repo_disp ([#3706](https://github.com/infinyon/fluvio/issues/3706))
* Properly pass owner references in conversions ([#3702](https://github.com/infinyon/fluvio/issues/3702))

### Other

* Move hub connector commands to fluvio-hub-util ([#3611](https://github.com/infinyon/fluvio/issues/3611))
* Smartmodule latest interface adoption ([#3661](https://github.com/infinyon/fluvio/issues/3661))
* Use individual cli test retries ([#3676](https://github.com/infinyon/fluvio/issues/3676))
* Use installation type in cli commands ([#3675](https://github.com/infinyon/fluvio/issues/3675))
* Ci, patch up workflow sequencing ([#3684](https://github.com/infinyon/fluvio/issues/3684))
* Fvm user interface improvements ([#3683](https://github.com/infinyon/fluvio/issues/3683))

### Removed

* Remove `fluvio update` command ([#3643](https://github.com/infinyon/fluvio/issues/3643))
* Deprecate `fluvio install` in place of `fvm` ([#3647](https://github.com/infinyon/fluvio/issues/3647))
* Deprecate`fluvio install` ([#3655](https://github.com/infinyon/fluvio/issues/3655))

## Platform Version 0.10.17 - 2023-10-30

### CI

* Release 0.10.17

### Other

* Fvm version command ([#3626](https://github.com/infinyon/fluvio/issues/3626))
* Fvm install help command with usage example ([#3629](https://github.com/infinyon/fluvio/issues/3629))
* Use local metadata for local clusters ([#3617](https://github.com/infinyon/fluvio/issues/3617))

### Removed

* Clean up GitHub action ([#3631](https://github.com/infinyon/fluvio/issues/3631))

## Platform Version 0.10.17-dev-1 - 2023-10-25

### Added

* Add `version` command to `smdk` and `cdk` ([#3571](https://github.com/infinyon/fluvio/issues/3571))
* Make consumer config public ([#3595](https://github.com/infinyon/fluvio/issues/3595))
* Install fluvio versions ([#3576](https://github.com/infinyon/fluvio/issues/3576))
* Fvm switch command ([#3597](https://github.com/infinyon/fluvio/issues/3597))
* Feat/move-specs-traits-to-fluvio ([#3598](https://github.com/infinyon/fluvio/issues/3598))
* Added local metadata store impl ([#3610](https://github.com/infinyon/fluvio/issues/3610))

### CI

* Post-release 0.10.16 ([#3590](https://github.com/infinyon/fluvio/issues/3590))
* Improve hub credential error message ([#3614](https://github.com/infinyon/fluvio/issues/3614))
* Set prelease version 0.10.17-dev-1 ([#3624](https://github.com/infinyon/fluvio/issues/3624))

### Changed

* Update fluvio-socket repo metadata ([#3592](https://github.com/infinyon/fluvio/issues/3592))
* Update rust-toolchain.toml ([#3594](https://github.com/infinyon/fluvio/issues/3594))

### Fixed

* Partially assert on cli output ([#3608](https://github.com/infinyon/fluvio/issues/3608))
* Replace `fvm` binary on `fvm self install` ([#3616](https://github.com/infinyon/fluvio/issues/3616))

### Other

* `fvm show` and `fvm current` command ([#3601](https://github.com/infinyon/fluvio/issues/3601))
* Set version as active after install ([#3604](https://github.com/infinyon/fluvio/issues/3604))
* Move file iterators to fluvio_storage crate ([#3613](https://github.com/infinyon/fluvio/issues/3613))

### Removed

* Remove unused deps on fvm ([#3606](https://github.com/infinyon/fluvio/issues/3606))

## Platform Version 0.10.16 - 2023-10-06

### Fixed

* Fluvio crash on downloading in macos-sonoma ([#3584](https://github.com/infinyon/fluvio/issues/3584))

### Other

* FVM `self` subcommand ([#3570](https://github.com/infinyon/fluvio/issues/3570))
* Use utc timestamp in cli-smdk-basic-test ([#3574](https://github.com/infinyon/fluvio/issues/3574))
* Use `surf` and `http-client` as workspace deps ([#3581](https://github.com/infinyon/fluvio/issues/3581))
* Move cli dep to workspace ([#3582](https://github.com/infinyon/fluvio/issues/3582))
* Fvm API Client and updated type definitions ([#3566](https://github.com/infinyon/fluvio/issues/3566))


## Platform Version 0.10.15 - 2023-09-28

### Added

* Update `fluvio hub` subcommands ([#3409](https://github.com/infinyon/fluvio/issues/3409))
* Add stdin input support for `smdk test` ([#3464](https://github.com/infinyon/fluvio/issues/3464))
* Multi partition consumer in sink connectors ([#3470](https://github.com/infinyon/fluvio/issues/3470))
* Print more error causes ([#3475](https://github.com/infinyon/fluvio/issues/3475))
* Add nonzero copy option ([#3519](https://github.com/infinyon/fluvio/issues/3519))
* Remove unnecessary generics ([#3529](https://github.com/infinyon/fluvio/issues/3529))
* Add `--truncate` arg to `fluvio consume` and pretty print json output ([#3551](https://github.com/infinyon/fluvio/issues/3551))
* Add generic signature to topic controller to make it easier to test ([#3445](https://github.com/infinyon/fluvio/issues/3445))
* Make k8 feature available in fluvio-run ([#3446](https://github.com/infinyon/fluvio/issues/3446))
* Reduce metadata item bound ([#3448](https://github.com/infinyon/fluvio/issues/3448))

### CI

* Update Zig to `v0.11.0` ([#3484](https://github.com/infinyon/fluvio/issues/3484))
* Update rust toolchain ([#3501](https://github.com/infinyon/fluvio/issues/3501))
* Clean up zig install ([#3508](https://github.com/infinyon/fluvio/issues/3508))
* Ci badge should report master status ([#3524](https://github.com/infinyon/fluvio/issues/3524))
* Point ci badge to staging ([#3526](https://github.com/infinyon/fluvio/issues/3526))
* Update k8 crates ([#3546](https://github.com/infinyon/fluvio/issues/3546))
* Add capability to start SC on read-only mode. Read-only mode me… ([#3525](https://github.com/infinyon/fluvio/issues/3525))
* Post release ([#3431](https://github.com/infinyon/fluvio/issues/3431))
* Configure fluvio-run build for armv7 ([#3444](https://github.com/infinyon/fluvio/issues/3444))

### Changed

* Update dep ([#3560](https://github.com/infinyon/fluvio/issues/3560))
* Update dep ([#3474](https://github.com/infinyon/fluvio/issues/3474))
* Update dep ([#3504](https://github.com/infinyon/fluvio/issues/3504))
* Update `fluvio-connector-derive` to syn@2 ([#3513](https://github.com/infinyon/fluvio/issues/3513))
* Update zstd@`v0.12.4` ([#3514](https://github.com/infinyon/fluvio/issues/3514))
* Update toml and wasmtime ([#3547](https://github.com/infinyon/fluvio/issues/3547))
* Update dep ([#3436](https://github.com/infinyon/fluvio/issues/3436))
* Update dep ([#3447](https://github.com/infinyon/fluvio/issues/3447))

### Fixed

* Install.sh, fluvio install/update, add arch/target overrides ([#3463](https://github.com/infinyon/fluvio/issues/3463))
* Enable connector crates tests in CI ([#3471](https://github.com/infinyon/fluvio/issues/3471))
* Cleanup format in some crates and fix fluvio cluster status wh… ([#3500](https://github.com/infinyon/fluvio/issues/3500))
* Provision topics in resource insufficient state if there is a ne… ([#3549](https://github.com/infinyon/fluvio/issues/3549))
* Run replica test on stable ([#3432](https://github.com/infinyon/fluvio/issues/3432))
* Re-enable skipped backwards compatibility tests ([#3434](https://github.com/infinyon/fluvio/issues/3434))

### Other

* Unify common dependencies ([#3442](https://github.com/infinyon/fluvio/issues/3442))
* Use generic client on sm migration and on k8 controllers ([#3468](https://github.com/infinyon/fluvio/issues/3468))
* Components tests on local cluster ([#3457](https://github.com/infinyon/fluvio/issues/3457))
* Include cli in docker image ([#3483](https://github.com/infinyon/fluvio/issues/3483))
* Expose use_cluster_ip cli flag ([#3480](https://github.com/infinyon/fluvio/issues/3480))
* Consume all partitions by default ([#3489](https://github.com/infinyon/fluvio/issues/3489))
* Fvm type definitions ([#3531](https://github.com/infinyon/fluvio/issues/3531))
* Use `SmartModuleRecord` instead of `Record` for `smdk generate` ([#3427](https://github.com/infinyon/fluvio/issues/3427))
* Unify common dependencies ([#3408](https://github.com/infinyon/fluvio/issues/3408))
* Migrate anyhow for replica assignment ([#3439](https://github.com/infinyon/fluvio/issues/3439))
* Sc k8 feature flag ([#3443](https://github.com/infinyon/fluvio/issues/3443))
* Quote protection for pw inconvenient chars ([#3451](https://github.com/infinyon/fluvio/issues/3451))
* Move spu smartmodule to controlplane only ([#3453](https://github.com/infinyon/fluvio/issues/3453))
* Decouple from K8MetaItem struct on fluvio-sc ([#3454](https://github.com/infinyon/fluvio/issues/3454))

### Removed

* Remove lazy_static in favor of once_cell ([#3466](https://github.com/infinyon/fluvio/issues/3466))
* Remove SmartModuleMigrationController ([#3472](https://github.com/infinyon/fluvio/issues/3472))
* Provides deprecation docs for warning ([#3488](https://github.com/infinyon/fluvio/issues/3488))
* Remove legacy install script ([#3503](https://github.com/infinyon/fluvio/issues/3503))
* Clean up partition creation path ([#3540](https://github.com/infinyon/fluvio/issues/3540))
* Fair replica scheduler ([#3545](https://github.com/infinyon/fluvio/issues/3545))
* Remove unnecessary test for PartitionMap ([#3438](https://github.com/infinyon/fluvio/issues/3438))
* Remove unnecessary dep in controlplane-metadata ([#3450](https://github.com/infinyon/fluvio/issues/3450))
* More cleanup ([#3452](https://github.com/infinyon/fluvio/issues/3452))


## Platform Version 0.10.14 - 2023-07-28

### Added

* Added smart engine memory limit ([#3407](https://github.com/infinyon/fluvio/issues/3407))
* Improve log message and add explicit matching on spu dispatcher loop ([#3413](https://github.com/infinyon/fluvio/issues/3413))
* "fluvio profile add" cmd ([#3419](https://github.com/infinyon/fluvio/issues/3419))

### CI

* Added topic-level deduplication mechanism ([#3385](https://github.com/infinyon/fluvio/issues/3385))
* Inject timestamp to record in SmartModule context ([#3389](https://github.com/infinyon/fluvio/issues/3389))

### Changed

* Update dep ([#3400](https://github.com/infinyon/fluvio/issues/3400))
* Update dep ([#3415](https://github.com/infinyon/fluvio/issues/3415))

### Fixed

* Replica assignment ([#3422](https://github.com/infinyon/fluvio/issues/3422))
* Put bash argument inside quotes in smartmodule publish ([#3401](https://github.com/infinyon/fluvio/issues/3401))
* Properly handle produce operation failure ([#3405](https://github.com/infinyon/fluvio/issues/3405))


## Platform Version 0.10.13 - 2023-07-13

### Added

* Log connector name and version on startup ([#3356](https://github.com/infinyon/fluvio/issues/3356))
* Added topic config ([#3350](https://github.com/infinyon/fluvio/issues/3350))
* Update syn to 2.0 on test crates ([#3366](https://github.com/infinyon/fluvio/issues/3366))
* Support time bound in lookback engine ([#3369](https://github.com/infinyon/fluvio/issues/3369))
* Added topic deduplication mechanism 1/2 ([#3392](https://github.com/infinyon/fluvio/issues/3392))

### Changed

* Update dep ([#3360](https://github.com/infinyon/fluvio/issues/3360))
* Update fluvio install URL to use Hub ([#3373](https://github.com/infinyon/fluvio/issues/3373))
* Update toolchain to new rust version ([#3394](https://github.com/infinyon/fluvio/issues/3394))

### Fixed

* Fix smdk publish with --push flag ([#3352](https://github.com/infinyon/fluvio/issues/3352))
* Fix --push flag on smdk and cdk publish ([#3370](https://github.com/infinyon/fluvio/issues/3370))
* Typo in chrono dep ([#3380](https://github.com/infinyon/fluvio/issues/3380))
* Fix re-usable workflow for publishing smartmodules ([#3347](https://github.com/infinyon/fluvio/issues/3347))
* Add `apiVersion` to connector template ([#3382](https://github.com/infinyon/fluvio/issues/3382))
* Use explicit `Result` type from std on generated code ([#3393](https://github.com/infinyon/fluvio/issues/3393))
* Ci, fix fluvio-run aarch-unknown-linux-musl release ([#3395](https://github.com/infinyon/fluvio/issues/3395))

### Other

* Migrate to OnceLock ([#3364](https://github.com/infinyon/fluvio/issues/3364))
* `HUB_API_BPKG_AUTH` as part of the `fluvio-hub-util` ([#3363](https://github.com/infinyon/fluvio/issues/3363))
* `Record` instance getters for timestamps ([#3387](https://github.com/infinyon/fluvio/issues/3387))
* Copy keys-value output behavior from `fluvio consume` to `smdk test` ([#3391](https://github.com/infinyon/fluvio/issues/3391))


## Platform Version 0.10.12 - 2023-06-26

### Added

* Fluvio-hub-protocol, tag util methods ([#3314](https://github.com/infinyon/fluvio/issues/3314))
* Added topic config ([#3345](https://github.com/infinyon/fluvio/issues/3345))

### Changed

* Update dependencies ([#3305](https://github.com/infinyon/fluvio/issues/3305))
* Update wasmtime to 10.0.1 ([#3344](https://github.com/infinyon/fluvio/issues/3344))

### Fixed

* Fluvio-socket, improve connect error message ([#3324](https://github.com/infinyon/fluvio/issues/3324))
* Lookback is not passed from connectors ([#3332](https://github.com/infinyon/fluvio/issues/3332))
* Don't require Cargo project to start connector from ipkg ([#3340](https://github.com/infinyon/fluvio/issues/3340))


## Platform Version 0.10.11 - 2023-06-09

### Added

* Report error if lookback is used on producer ([#3310](https://github.com/infinyon/fluvio/issues/3310))
* Support `lookback` in test ([#3317](https://github.com/infinyon/fluvio/issues/3317))
* Added `look_back` to smartmodule proc macro ([#3276](https://github.com/infinyon/fluvio/issues/3276))
* Support `look_back` on SmartEngine ([#3304](https://github.com/infinyon/fluvio/issues/3304))
* Support `look_back` on SPU ([#3306](https://github.com/infinyon/fluvio/issues/3306))
* Fluvio-schema, batch id ([#3283](https://github.com/infinyon/fluvio/issues/3283))
* Add option to run publish without build again ([#3296](https://github.com/infinyon/fluvio/issues/3296))

### Fixed

* Use dynamic value for `LogLevel` on deployment ([#3312](https://github.com/infinyon/fluvio/issues/3312))

### Other

* Provide a rich readme for fluvio crate ([#3303](https://github.com/infinyon/fluvio/issues/3303))
* Measure metrics on `look_back` calls ([#3311](https://github.com/infinyon/fluvio/issues/3311))


## Platform Version 0.10.10 - 2023-05-26

### Added

* Add more multiplexing_test ([#3249](https://github.com/infinyon/fluvio/issues/3249))
* Cdk deploy log level support ([#3278](https://github.com/infinyon/fluvio/issues/3278))
* Introduce zstd compression ([#3185](https://github.com/infinyon/fluvio/issues/3185))

### CI

* Update connector-publish.yml ([#3255](https://github.com/infinyon/fluvio/issues/3255))
* Prebuild artifacts for publish and deploy ([#3252](https://github.com/infinyon/fluvio/issues/3252))
* Cleanup after publishing packages ([#3259](https://github.com/infinyon/fluvio/issues/3259))
* Fix udeps issue ([#3280](https://github.com/infinyon/fluvio/issues/3280))

### Fixed
* Improve error logs while rendering interpolated strings ([#3266](https://github.com/infinyon/fluvio/issues/3266))

### Other

* Use kebab case for group names on hub packages ([#3264](https://github.com/infinyon/fluvio/issues/3264))
* Improve Feedback on CDK ([#3243](https://github.com/infinyon/fluvio/issues/3243))
* Set compression algorithm behind feature flag ([#3275](https://github.com/infinyon/fluvio/issues/3275))

## Platform Version 0.10.9 - 2023-05-11

### Added

* Simplified control plane api ([#3073](https://github.com/infinyon/fluvio/issues/3073))
* Add helper method to get utf8 from data ([#3236](https://github.com/infinyon/fluvio/issues/3236))
* Move x records outputed from smdk test to --verbose #3169 ([#3234](https://github.com/infinyon/fluvio/issues/3234))
* Add message about record too large on produce cli ([#3242](https://github.com/infinyon/fluvio/issues/3242))

### CI

* `cdk build` uses a default target ([#3222](https://github.com/infinyon/fluvio/issues/3222))
* `cdk test` and `cdk deploy` uses a default target ([#3247](https://github.com/infinyon/fluvio/issues/3247))
* Remove existing `.hub` when packing connectors ([#3227](https://github.com/infinyon/fluvio/issues/3227))
* Add materialize view rfc ([#3232](https://github.com/infinyon/fluvio/issues/3232))

### Changed

* Update third party dep ([#3220](https://github.com/infinyon/fluvio/issues/3220))

### Other

* Use current git rev for generated connector project ([#3225](https://github.com/infinyon/fluvio/issues/3225))
* Use `.hub` and `.DS_Store` for gitignore ([#3239](https://github.com/infinyon/fluvio/issues/3239))
* Ensure readme is included in manifest ([#3230](https://github.com/infinyon/fluvio/issues/3230))

### Removed

* Remove deprecated code related to clap ([#3228](https://github.com/infinyon/fluvio/issues/3228))
* Update third party and remove circular dep ([#3241](https://github.com/infinyon/fluvio/issues/3241))

## Platform Version 0.10.8 - 2023-05-03

### Added

* Add toml-diff to workspace ([#3191](https://github.com/infinyon/fluvio/issues/3191))
* Secrets in connector meta ([#3194](https://github.com/infinyon/fluvio/issues/3194))
* Add support to string interpolation ([#3138](https://github.com/infinyon/fluvio/issues/3138))
* Support k8 v1.26 ([#3196](https://github.com/infinyon/fluvio/issues/3196))
* Added keymgmt roundtrip test ([#3199](https://github.com/infinyon/fluvio/issues/3199))
* Add binary to manifest of connector meta ([#3203](https://github.com/infinyon/fluvio/issues/3203))
* Add max_bytes to connector consumer option ([#3209](https://github.com/infinyon/fluvio/issues/3209))

### CI

* Use workspace deps for serde_yaml ([#3140](https://github.com/infinyon/fluvio/issues/3140))
* Update cargo-generate to release version ([#3166](https://github.com/infinyon/fluvio/issues/3166))
* Updates for rust 1.69 ([#3179](https://github.com/infinyon/fluvio/issues/3179))
* Post release 0.10.7 notes ([#3176](https://github.com/infinyon/fluvio/issues/3176))

### Changed

* Update dep ([#3160](https://github.com/infinyon/fluvio/issues/3160))

### Fixed

* Enable test again on CI ([#3132](https://github.com/infinyon/fluvio/issues/3132))
* Properly close multiplexer ([#3170](https://github.com/infinyon/fluvio/issues/3170))
* Minor code changes for pem 2 dep update ([#3172](https://github.com/infinyon/fluvio/issues/3172))
* Fix toolchain reference ([#3180](https://github.com/infinyon/fluvio/issues/3180))
* Fix aggregate-initial description and organize Producer/Consumer options ([#3177](https://github.com/infinyon/fluvio/issues/3177))
* Use `.hub` over `hub` and delete if exists ([#3193](https://github.com/infinyon/fluvio/issues/3193))
* Fix fluvio hub utils ([#3198](https://github.com/infinyon/fluvio/issues/3198))
* Add tests to `SecretName` struct ([#3200](https://github.com/infinyon/fluvio/issues/3200))

### Other

* Parse input as boolean ([#3157](https://github.com/infinyon/fluvio/issues/3157))
* Improve modularity of smartengine ([#3152](https://github.com/infinyon/fluvio/issues/3152))
* Explicit set of errors to retry ([#3137](https://github.com/infinyon/fluvio/issues/3137))
* K8ClusterStateDispatcher start returns handle ([#3173](https://github.com/infinyon/fluvio/issues/3173))
* Upgrade wasmtime to 0.8.0 and dep ([#3175](https://github.com/infinyon/fluvio/issues/3175))
* Introduce utilily to check k8s resource names ([#3163](https://github.com/infinyon/fluvio/issues/3163))
* Clean up dep ([#3189](https://github.com/infinyon/fluvio/issues/3189))
* Move more dep to workspace ([#3190](https://github.com/infinyon/fluvio/issues/3190))
* Introduce `apiVersion` to `ConnectorConfig` in order to allow backward c… ([#3206](https://github.com/infinyon/fluvio/issues/3206))

### Removed

* Remove manual enum encoding/decoding ([#3165](https://github.com/infinyon/fluvio/issues/3165))
* Remove unused dep in storage and move dep to workspace ([#3181](https://github.com/infinyon/fluvio/issues/3181))
* Remove package ([#3188](https://github.com/infinyon/fluvio/issues/3188))

## Platform Version 0.10.7 - 2023-04-17
* Apply SmartModule Transformation For Producer ([#3014](https://github.com/infinyon/fluvio/issues/3014))
* Docs, Example of aggregation sm with initial value ([#3126](https://github.com/infinyon/fluvio/issues/3126))
* Smdk, Use relative paths in generated package-meta ([#3123](https://github.com/infinyon/fluvio/issues/3123))
* Add support to detect secret on sequences ([#3131](https://github.com/infinyon/fluvio/issues/3131))
* Multiple dependency updates

## Platform Version 0.10.6 - 2023-03-31
* Pin to specific rust release ([#2967](https://github.com/infinyon/fluvio/issues/2967))
* Added target info on `cdk publish` ([#3075](https://github.com/infinyon/fluvio/issues/3075))
* Supply `arch` tag on publish if not set ([#3080](https://github.com/infinyon/fluvio/issues/3080))
* Add `-p package_name` in `cdk publish` ([#3097](https://github.com/infinyon/fluvio/issues/3097))
* Remove unnecessary bounds for encoder and decoder derive macro ([#3030](https://github.com/infinyon/fluvio/issues/3030))
* Add target option to connector download ([#3079](https://github.com/infinyon/fluvio/issues/3079))
* CLI forward and compatibility ([#3048](https://github.com/infinyon/fluvio/issues/3048))
* SmartModule CLI watch ([#3064](https://github.com/infinyon/fluvio/issues/3064))
* Disallow untagged or named enum without constant mapping ([#3061](https://github.com/infinyon/fluvio/issues/3061))
* Use consistent naming for connector config example file ([#3077](https://github.com/infinyon/fluvio/issues/3077))
* Fluvio hub, use hubref for nonstandard configs ([#3086](https://github.com/infinyon/fluvio/issues/3086))
* Relax `cdk` secrets validation ([#3093](https://github.com/infinyon/fluvio/issues/3093))
* Enabled `--file` and `--key-separator` to be used together, fix `--key` handling when producing lines ([#3092](https://github.com/infinyon/fluvio/issues/3092))
* Fail fast if socket is stale ([#3054](https://github.com/infinyon/fluvio/issues/3054))
* Set max api version for derived stream ([#3041](https://github.com/infinyon/fluvio/issues/3041))
* Disable default `fluvio` features for sdk ([#3098](https://github.com/infinyon/fluvio/issues/3098))

## Platform Version 0.10.5 - 2023-02-28
* Upload bpkg logic ([#3028](https://github.com/infinyon/fluvio/issues/3028))
* Remove secrets and parameters from connector meta config ([#3022](https://github.com/infinyon/fluvio/issues/3022))
* Migrate fluvio admin api to anyhow ([#3016](https://github.com/infinyon/fluvio/issues/3016))
* Enhance deploy options ([#2968](https://github.com/infinyon/fluvio/issues/2968))
* Retry monitoring if there is a failure ([#2975](https://github.com/infinyon/fluvio/issues/2975))
* Support secrets in connector sdk ([#2983](https://github.com/infinyon/fluvio/issues/2983))
* Fluvio connector support update ([#2992](https://github.com/infinyon/fluvio/issues/2992))
* Cdk publish to hub ([#2979](https://github.com/infinyon/fluvio/issues/2979))
* Optimize smdk Smoke Test ([#2972](https://github.com/infinyon/fluvio/issues/2972))
* Add bpkg_token to publish workflow ([#3004](https://github.com/infinyon/fluvio/issues/3004))
* Fluvio-hub-x, update dep cargo_toml to 0.15 ([#3007](https://github.com/infinyon/fluvio/issues/3007))
* Publicly expose partitioning structs and traits ([#2969](https://github.com/infinyon/fluvio/issues/2969))
* Remove v1 topic spec ([#2987](https://github.com/infinyon/fluvio/issues/2987))

## Platform Version 0.10.4 - 2022-01-23
* Add 'cdk test' command ([#2948](https://github.com/infinyon/fluvio/issues/2948))
* Add download connector package command ([#2944](https://github.com/infinyon/fluvio/issues/2944))
* Add Hub support for installing binary packages ([#2942](https://github.com/infinyon/fluvio/issues/2942))
* Add support for OpenSSL as an optional dependency for Fluvio ([#2923](https://github.com/infinyon/fluvio/issues/2923))
* Add support for local deploy from `ipkg` file for Connector Development Kit (CDK) ([#2939](https://github.com/infinyon/fluvio/issues/2939))
* Add support custom configs in connector SDK ([#2910](https://github.com/infinyon/fluvio/issues/2910))
* Add cluster shutdown support ([#2912](https://github.com/infinyon/fluvio/issues/2912))
* Add sink connector support to SDK ([#2906](https://github.com/infinyon/fluvio/issues/2906))
* Fix `read_records` to truncate `max_offset` to `end_offset` of current segment if it is larger than that so we can read records of that segment without failures ([#2950](https://github.com/infinyon/fluvio/issues/2950))
* Fix use `i64` over `u64` for fuel measure to align with wasmtime ([#2945](https://github.com/infinyon/fluvio/issues/2945))
* Fix use `anyhow` over `thiserror` ([#2916](https://github.com/infinyon/fluvio/issues/2916))
* Fix CLI help text typo ([#2918](https://github.com/infinyon/fluvio/issues/2918))
* Fix replace `Smart Module` ocurrences with `SmartModule` ([#2913](https://github.com/infinyon/fluvio/issues/2913))
* Fix split _Custom Config_ and _Common Config_ Separate custom config from common config ([#2917](https://github.com/infinyon/fluvio/issues/2917))
* Fix exposes `retry` function by re-exporting for common connector crate ([#2922](https://github.com/infinyon/fluvio/issues/2922))
* Fix recover from invalid segments ([#2909](https://github.com/infinyon/fluvio/issues/2909))
* Fix remove connector metadata and API ([#2887](https://github.com/infinyon/fluvio/issues/2887))

## Platform Version 0.10.3 - 2022-12-16
* Add `fluvio cluster status` ([#2824](https://github.com/infinyon/fluvio/issues/2824))
* Display fetch status ([#2872](https://github.com/infinyon/fluvio/issues/2872))
* Emphasize flush in docs ([#2850](https://github.com/infinyon/fluvio/issues/2850))
* fix measurement of outbound metrics in SmartEngine ([#2865](https://github.com/infinyon/fluvio/issues/2865))
* Fix `smdk` load and test when package name contains `-` ([#2863](https://github.com/infinyon/fluvio/issues/2863))

## Platform Version 0.10.2 - 2022-12-01
* Add hub private packages ([#2828](https://github.com/infinyon/fluvio/issues/2828))
* Update wasmtime to 0.3.0 ([#2831](https://github.com/infinyon/fluvio/issues/2831))
* Benchmarking tool behaviour on timeout ([#2838](https://github.com/infinyon/fluvio/issues/2838))
* Correct producer throughput measurement ([#2839](https://github.com/infinyon/fluvio/issues/2839))
* Release batches lock ([#2840](https://github.com/infinyon/fluvio/issues/2840))

## Platform Version 0.10.1 - 2022-11-18
* Improve performance for `RecordAccumulator` in `batches` ([#2799](https://github.com/infinyon/fluvio/pull/2799))
* Replace `PartitionId`, `PartitionCount` and `ReplicationFactor` aliased types to use `u32` over `i32` ([#2799](https://github.com/infinyon/fluvio/pull/2799))
* Standardize reading records from CLI input ([#2756](https://github.com/infinyon/fluvio/pull/2756))
* Smdk publish signing error w/ cross-fs tmp file ([#2767](https://github.com/infinyon/fluvio/pull/2767))
* Smkd template dir includes default gitignore ([#2768](https://github.com/infinyon/fluvio/pull/2768))
* Clap-command updates ([#2766](https://github.com/infinyon/fluvio/pull/2766))
* Reduce wasm size by stripping symbols ([#2774](https://github.com/infinyon/fluvio/pull/2774))
* Remove old comment from src ([#2778](https://github.com/infinyon/fluvio/pull/2778))
* Ci-dev workflow fix ([#2783](https://github.com/infinyon/fluvio/pull/2783))
* Fluvio-test harness bug ([#2790](https://github.com/infinyon/fluvio/pull/2790))
* Added chain support to producer ([#2753](https://github.com/infinyon/fluvio/pull/2753))
* Capability to validate WASM files ([#2760](https://github.com/infinyon/fluvio/pull/2760))
* Added smartmodule chain support for consumer ([#2759](https://github.com/infinyon/fluvio/pull/2759))
* Benches with criterion and dedicated workflow ([#2770](https://github.com/infinyon/fluvio/pull/2770))
* Benches for SmartModuleInput encoding ([#2773](https://github.com/infinyon/fluvio/pull/2773))
* Use `content: write` permission for benchmarks ([#2775](https://github.com/infinyon/fluvio/pull/2775))
* Check for `Smart.toml` file to be present in cwd ([#2739](https://github.com/infinyon/fluvio/pull/2739))
* Introduce `ByteBuf` for `SmartModuleSpec` ([#2738](https://github.com/infinyon/fluvio/pull/2738))
* Fluvio cli update check ([#2679](https://github.com/infinyon/fluvio/pull/2679))
* Update cargo generate to use `Default` ([#2786](https://github.com/infinyon/fluvio/pull/2786))
* Moved versioned socket to fluvio-socket crate ([#2797](https://github.com/infinyon/fluvio/pull/2797))
* Added chaining support to smdk test ([#2784](https://github.com/infinyon/fluvio/pull/2784))
* Added chaining support to fluvio-cli ([#2812](https://github.com/infinyon/fluvio/pull/2812))
* Add additional test for encoding/decoding version ([#2761](https://github.com/infinyon/fluvio/pull/2761))
* Update dev version ([#2817](https://github.com/infinyon/fluvio/pull/2817))

## Platform Version 0.10.0 - 2022-10-24
* Add throughput control to fluvio producer ([#2512](https://github.com/infinyon/fluvio/pull/2512))
* Added blocking on Producer if the batch queue is full ([#2562](https://github.com/infinyon/fluvio/pull/2562))
* Initial support for SmartEngine v2 ([#2610](https://github.com/infinyon/fluvio/pull/2610))
* SmartModule chaining ([#2618](https://github.com/infinyon/fluvio/pull/2618))
* Add `smdk` ([#2632](https://github.com/infinyon/fluvio/pull/2632))
* Support to build `SmartModules` using `smdk build` ([#2638](https://github.com/infinyon/fluvio/pull/2638))
* SmartModule Load Phase 1 ([#2639](https://github.com/infinyon/fluvio/pull/2639))
* Add fluvio sm download ([#2656](https://github.com/infinyon/fluvio/pull/2656))
* Add instrumentation to client producer ([#2717](https://github.com/infinyon/fluvio/pull/2717))
* Added metrics to smartengine ([#2726](https://github.com/infinyon/fluvio/pull/2726))
* Add otel to consumer ([#2725](https://github.com/infinyon/fluvio/pull/2725))
* Add record counters to spu ([#2731](https://github.com/infinyon/fluvio/pull/2731))
* Metric endpoint ([#2737](https://github.com/infinyon/fluvio/pull/2737))
* Update clap to v4 ([#2670](https://github.com/infinyon/fluvio/pull/2670))
* `smdk` test Mode ([#2636](https://github.com/infinyon/fluvio/pull/2636))
* Generate SmartModules using `smdk` ([#2630](https://github.com/infinyon/fluvio/pull/2630))
* Add `smdk` publish ([#2657](https://github.com/infinyon/fluvio/pull/2657))
* Add `smdk` install support in CLI and release ([#2648](https://github.com/infinyon/fluvio/pull/2648))
* Rewrite `test-crate-version` in Rust ([#2595](https://github.com/infinyon/fluvio/pull/2595))
* `smdk` generate template ([#2677](https://github.com/infinyon/fluvio/pull/2677))
* Add non-interactive `smdk generate` flow ([#2693](https://github.com/infinyon/fluvio/pull/2693))
* Use `localhost` as `proxy-addr` for k8s cluster on macOS ([#2740](https://github.com/infinyon/fluvio/pull/2740))
* Add prompt for project group in `smdk generate` ([#2746](https://github.com/infinyon/fluvio/pull/2746))
* Use dynamic local port for k8 port forwarding ([#2578](https://github.com/infinyon/fluvio/pull/2578))
* Producer stat ([#2743](https://github.com/infinyon/fluvio/pull/2743))

## Platform Version 0.9.33 - 2022-08-10
* Added `DeliverySemantic` to `fluvio-cli`. ([#2508](https://github.com/infinyon/fluvio/pull/2508))
* CLI: Added ability to delete multiple connectors, smartmodules and topics with one command. ([#2427](https://github.com/infinyon/fluvio/issues/2427))
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
