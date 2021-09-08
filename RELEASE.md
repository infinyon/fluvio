# Release process


## Pre-release

Prior to releasing, the release manager should check the following:

Update fluvio website doc (master):

- [ ] Sample code homepage is correct (Rust/Node/Python).
- [ ] Review the "getting started" docs.
- [ ] Update Any API related docs.
- [ ] Review rest of doc to ensure they are up to date.

Other dependent repos:
- [ ] Update `infinyon/fluvio-smartstream-template` if needed.

## Release

First Lock master branch (TBD)

- [ ] Publish all public crates.
  - [ ] `fluvio` and any dependencies
  - [ ] `fluvio-smartstream`
- [ ] Update `CHANGELOG.md` with release date.
- [ ] Run the `release.yml` workflow on GitHub Actions.
- [ ] Wait for workflow to complete successfully and apply `vX.Y.Z` tag to git.
- [ ] Update fluvio website to stable.

Unlock Master branch (TBD)

## Post-release

After performing the release, the release manager should do the following in order
to prepare for the next release and announce the current release to the community:

- [ ] Bump up the version in the `VERSION` file.
- [ ] Add UNRELEASED section for new version to `CHANGELOG.md`.
- [ ] Announce the release on Discord and Twitter.

# Release recovery process

## Cleanup failed Release
In the event that the release automation fails, there is manual cleanup required before re-running the automation.

### Delete artifacts:
- Docker Hub
  - Delete the image tag corresponding to the release VERSION  
- S3
  - Delete the version directory for `fluvio` and `fluvio-run` artifacts
  - s3://packages.fluvio.io/v1/packages/fluvio/fluvio/<VERSION>
  - s3://packages.fluvio.io/v1/packages/fluvio/fluvio-run/<VERSION>
- Github Releases
  - Delete the latest release
  - Delete any DRAFT releases

### Fix the installer
- `fluvio install fluvio-package`
- `fluvio package tag fluvio:x.y.z --tag=stable --force`
- Remove last entry in fluvio-run meta.json
  - s3://packages.fluvio.io/v1/packages/fluvio/fluvio-run/meta.json 
  - This should be a regular release tag (x.y.z), not a dev tag (x.y.z+gitcommit)
  - Confirm that the installation script works
    - `curl -fsS https://packages.fluvio.io/v1/install.sh | bash`
