# Release process

Prior to releasing, the release manager should check the following:

- [ ] Confirm that the sample code on the [fluvio.io] homepage is correct (Rust/Node/Python)
- [ ] Review the "getting started" docs on fluvio.io/docs

[fluvio.io]: https://fluvio.io

To actually perform the release, the core steps are:

- Run the `release.yml` workflow on GitHub Actions
- Wait for workflow to complete successfully and apply `vX.Y.Z` tag to git
- Publish any crates `fluvio` client depends on, followed by `fluvio` crate itself
- Fast-forward `stable` branch to match `master` (may become automated)
- Push new commit with updated `VERSION` and `CHANGELOG.md` files

After performing the release, the release manager should do the following in order
to prepare for the next release and announce the current release to the community:

- [ ] Bump up the version in the `VERSION` file.
- [ ] Add UNRELEASED section for new version to `CHANGELOG.md`.
- [ ] Announce the release on Discord and Twitter.
