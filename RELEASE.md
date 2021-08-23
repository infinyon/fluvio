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
- [ ] Create PR from master to stable branch.  Get approval and merge it.
- [ ] Update fluvio website to stable.

Unlock Master branch (TBD)

## Post-release

After performing the release, the release manager should do the following in order
to prepare for the next release and announce the current release to the community:

- [ ] Bump up the version in the `VERSION` file.
- [ ] Add UNRELEASED section for new version to `CHANGELOG.md`.
- [ ] Announce the release on Discord and Twitter.
