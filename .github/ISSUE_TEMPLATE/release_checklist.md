---
name: New Release Checklist
about: Process to follow for running the Fluvio release
title: "[Release Checklist]:"
labels: tracking 
assignees: ''

---

# Release Checklist

The following checklist covers all the steps for running release.

For more detail, refer to [`RELEASE.md`](https://github.com/infinyon/fluvio/blob/master/RELEASE.md)

- [ ] Create a [new issue](https://github.com/infinyon/fluvio/issues/new?template=release_checklist.md) with this checklist template
- [ ] Review all the [Getting Started](https://www.fluvio.io/docs/get-started/mac/) and [API](https://www.fluvio.io/api/) docs. Verify if they are up to date or will need updates
- [ ] Inform team that release is about to begin
- [ ] Ensure no merges are in flight
- [ ] Create a PR for release
  - [ ] Update `VERSION` and `CHANGELOG.md` files
  - [ ] Merge the PR
- [ ] Run the [`Release` workflow in Github Actions](https://github.com/infinyon/fluvio/actions/workflows/release.yml) (Retry at least once if failure)  
- [ ] Verify that the [crates publish to crates.io](https://github.com/infinyon/fluvio/actions/workflows/publish_crates.yml) succeeds (Retry at least once if failure)
- [ ] Announce new release in Discord
- [ ] Create a PR for post release updates and use `release_template.md` close this issue
  - Add form data `template=release_template.md` to the PR URL while creating in order to use template
  - e.g. https://github.com/infinyon/fluvio/compare/master...username:your-branch?expand=1&template=release_template.md

### Generating Release notes

To get a starting point for generating release notes

```bash
git cliff 673e60c0..HEAD > changes.md
```
