# Release process

To do a "full release", the core steps are:
* Publish all updated crates (this may be a little annoying until it's automated).
* Bump up version `VERSION` file.
* Kick off release flow with github `release` action.
* Rebase master into stable

## Release Checklist

There might be only a few steps to release but the release manager should
verify the following:

- [ ] Publish all updated crates.
- [ ] Bump up version `VERSION` file.
- [ ] Review/run sample code on Home Page - Rust/Node/Python
- [ ] Review getting started docs on fluvio.io/docs
- [ ] Rebase master into stable
- [ ] Kick off release flow with github `release` action.
- [ ] Announce the release on Discord and twitter
- [ ] Update `VERSION` file to `alpha-0` for the next release
- [ ] Add Unreleased section to `CHANGELOG.md`.
