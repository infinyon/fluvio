#[test]
fn test_pinned_version_channelissue() {
    use assert_cmd::prelude::*;
    use predicates::prelude::*;
    use std::process::Command;

    // Note: this could be brittle if in the future this version is no longer a resolvable version
    let pinned_channel = String::from("0.9.18");

    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    // forces it to use the default channel config location because if not it will fall-back to dev channel
    // TODO: when passing a non-default channel config location is supported then switch to test specific config file
    // rather than using default channel config
    cmd.env("FLUVIO_FRONTEND", "true");
    cmd.arg("version").arg("create").arg(&pinned_channel);
    cmd.assert().success();

    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    cmd.env("FLUVIO_FRONTEND", "true");
    cmd.arg("version").arg("switch").arg(&pinned_channel);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(&pinned_channel));

    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    cmd.env("FLUVIO_FRONTEND", "true");
    cmd.arg("update");
    cmd.assert()
        .failure()
        .stdout(predicate::str::contains("Unsupported Feature"));
}
