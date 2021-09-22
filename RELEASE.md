# Release process

The full process for releasing Fluvio consists of 3 separate workflows, which are detailed below in their own sections:

1. [Pre-release workflow](#pre-release-workflow)
2. [Release workflow](#release-workflow)
3. [Post-release workflow](#post-release-workflow)

In the event that a release needs to be un-released, please follow the [Release recovery process](#release-recovery-process)

## Pre-release workflow

This is a mostly **manual** workflow

Prior to releasing, the release manager should check the following:

Review Fluvio website:
- [ ] Review the "Getting Started" docs.
  - [ ] https://www.fluvio.io/docs/get-started/linux/
  - [ ] https://www.fluvio.io/docs/get-started/mac/
  - [ ] https://www.fluvio.io/docs/get-started/raspberry/
  - [ ] https://www.fluvio.io/docs/get-started/cloud/
- [ ] Sample code is correct (API docs are correct)
  - [ ] https://www.fluvio.io/api/fluvio/rust/
  - [ ] https://www.fluvio.io/api/fluvio/python/
  - [ ] https://www.fluvio.io/api/fluvio/node/
  - [ ] https://www.fluvio.io/api/fluvio/java/
- [ ] Review rest of doc to ensure they are up to date.
  - https://www.fluvio.io/docs/

Other dependent repos:
- [ ] Update [`infinyon/fluvio-smartstream-template`](https://github.com/infinyon/fluvio-smartstream-template) if needed.

## Release workflow

This is a mostly **automated** workflow
### Let the team know before starting
Send a message in the Infinyon Slack `#dev` channel to let the team know release is about to occur.

The team should understand that **No PR merges unrelated to release should occur during this time**

### Run the Release automation

Run the [`release.yml` Github Actions workflow](https://github.com/infinyon/fluvio/actions/workflows/release.yml)

This workflow will:

- Publish all public crates in the `crates` directory
  - [`fluvio`](https://crates.io/crates/fluvio) and any dependencies
  - [`fluvio-smartstream`](https://crates.io/crates/fluvio-smartstream) and any dependencies
  - The rest of the crates w/ a version number that isn't `v0.0.0`
- Push Fluvio docker image release tags to Docker Hub
  - https://hub.docker.com/r/infinyon/fluvio
- Publish fluvio artifacts to AWS S3 (via `fluvio package`) for installer
- Create [Github Release](https://github.com/infinyon/fluvio/releases) for the current version (w/ Release notes derived from `CHANGELOG.md`)
- Create a git tag on the commit in Fluvio repo that was just released

#### In event of failure in Release workflow
If any steps fail in `release.yml`, try to run it a 2nd time before asking in `#dev`.

This workflow has been written to be idempotent and so will only perform work if necessary even if run multiple times.

## Post-release workflow

This is a mostly **manual** workflow 

After performing the release, the release manager should do the following in order
to prepare for the next release and announce the current release to the community:


1. [ ] Update files in Fluvio repo, open PR and merge
    - Update `VERSION` file for next release
      - [ ] Minor version bump the version in the `VERSION` file.
    - Update `CHANGELOG.md` file for next release
      - [ ] Add UNRELEASED section (matching value as `VERSION` file) to `CHANGELOG.md` at top of file (but under the `# Release Notes` header)
        - ```## Platform Version X.Y.Z - UNRELEASED```
      - [ ] For version just released, replace `UNRELEASED` date with current date (YYY-MM-dd) in `CHANGELOG.md`.

2. [ ] Announce the release on Discord (`#announcements` channel) and Twitter ([`@fluvio_io`](https://twitter.com/fluvio_io) user).

    - Discord announcement Template:
      - Aim to announce ~3 features max. If we have more, point out that release notes includes more)
      ```
      Fluvio vX.Y.Z is out! 🎉
      This release includes:
      * (Changelog feature 1)
      * (Changelog feature 2)
      * (Changelog feature 3)

      Link to full release notes 📋
      https://github.com/infinyon/fluvio/releases/tag/vX.Y.Z
      ```

3. Send another message in Infinyon Slack to let the team know that release is complete (so we can merge PRs again!)

# Release recovery process

This is a completely **manual** workflow

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
