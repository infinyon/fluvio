---
name: New Release Checklist
about: Process to follow for running the Fluvio release
title: "[Release Checklist]:"
labels: tracking
assignees: ''

---

# Release Checklist

The following checklist covers all the steps for running release.

First, a pre-release is published which is used to ensure all systems operate
correctly in development environments. Once the pre-release is confirmed to be
working, the final release is published.

For more detail, refer to [`RELEASE.md`](https://github.com/infinyon/fluvio/blob/master/RELEASE.md)

- [ ] Create a [new issue](https://github.com/infinyon/fluvio/issues/new?template=release_checklist.md) with this checklist template
- [ ] Review all the [Getting Started](https://www.fluvio.io/docs/get-started/mac/) and [API](https://www.fluvio.io/api/) docs. Verify if they are up to date or will need updates
- [ ] Inform team that release is about to begin
- [ ] Ensure no merges are in flight

## Pre-Release

- [ ] Run the [`Release` workflow in Github Actions](https://github.com/infinyon/fluvio/actions/workflows/release.yml), **make sure the `pre-release` check is set**. (Retry at least once if failure)
- [ ] Perform a pre-release test in a development environment

## Stable Release

If all systems are operational and no bugs are found in the pre-release, the final release can be published.

- [ ] Create a PR for release
  - [ ] Update `VERSION` and `CHANGELOG.md` files (do not place a \n in the VERSION file, it breaks the CI)
  - [ ] Merge the PR
- [ ] Run the [`Release` workflow in Github Actions](https://github.com/infinyon/fluvio/actions/workflows/release.yml)
**without the `pre-release` check**.
- [ ] Verify that the [crates publish to crates.io](https://github.com/infinyon/fluvio/actions/workflows/publish_crates.yml) succeeds (Retry at least once if failure)
- [ ] Announce new release in Discord
- [ ] Update the [Fluvio docs](https://www.fluvio.io/) with the new release.
- [ ] Publish a `This Week in Fluvio` post about the new release on the [Fluvio news](https://www.fluvio.io/news) page.
- [ ] To close this issue, create a PR for post release updates and use `release_template.md`
  - Add form data `template=release_template.md` to the PR URL while creating in order to use template
  - e.g. https://github.com/infinyon/fluvio/compare/master...username:your-branch?expand=1&template=release_template.md
  - or `gh pr create --template=release_template.md` a closes issue #x in the body will automatically close this issue on merge.

### Generating Release notes

To get a starting point for generating release notes the view-changelog script can be used.
git cliff is used to generate a changelog update. The view-changelog script can be used to view
the new entries or modifty the CHANGELOG.md file.

To view the changelog updates:

`actions/view-changelog.sh`

or to prepend the updates (some post editing is still usually required):

`actions/view-changelog.sh modify`
