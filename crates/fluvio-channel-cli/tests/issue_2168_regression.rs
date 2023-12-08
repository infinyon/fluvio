// In this test which resolves [issue_2168](https://github.com/infinyon/fluvio/issues/2168),
// we want to verify that attempting to create a version that cannot be resolved will
// return the correct error message ("Unable to resolve version") AND will also not save the
// invalid version to the channel config file (~/.fluvio/config)
//
// To do so, we create a random version name that is likely to not already be in the config file.
// We verify this by running `fluvio-channel version list` and ensuring the random channel does not
// already exist.
//
// If it does not exist, we attempt to create the version with `fluvio-channel
// version create <random_channel_name>`, verify we get the expected error message and then list
// the config file contents again to verify the invalid version still does not exist.
//
// Finally, to clean up, we attempt to run `fluvio-channel version delete <random_channel_name>`
// just in case the version was indeed written to the file.
// This should never succeed and we assert that.
//
// fluvio-channel is deprecated, minimal maintenance ignore this test
#[ignore]
#[test]
fn issue_2168_regression_test() {
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::process::Command; // Run programs
    use std::env;
    use rand::Rng;

    //we want to check if we're in the CI environment.
    if env::var("CI").is_ok() {
        //if so, we should initialize the channel config file
        //at this point, this should always be true since Github Actions always sets this as such
        let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
        cmd.arg("version").arg("switch").arg("stable");
        cmd.assert().success();
    }
    //channel name characters to be used at random
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789";

    //the choice of a 10 character name is arbitrary
    const RANDOM_CHANNEL_NAME_LENGTH: usize = 10;

    //create a random 10 character channel name for this test
    let random_channel_name: String = (0..RANDOM_CHANNEL_NAME_LENGTH)
        .map(|_| {
            let idx = rand::thread_rng().gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    //verify our test channel name is not currently in the channel config
    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    cmd.arg("version").arg("list");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(&random_channel_name).count(0));

    //if we get this far, we can attempt to create the channel version
    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    cmd.arg("version").arg("create").arg(&random_channel_name);
    cmd.assert()
        .failure()
        .stdout(predicate::str::contains("Unable to resolve version"));

    //verify our test channel name STILL IS NOT in the channel config
    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    cmd.arg("version").arg("list");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(&random_channel_name).count(0));

    //just in case, clean up by deleting the random channel
    //from the channel config file (it should NOT be found)
    let mut cmd = Command::cargo_bin("fluvio-channel").expect("fluvio-channel binary");
    cmd.arg("version").arg("delete").arg(&random_channel_name);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "Release channel \"{}\" not found",
            &random_channel_name
        )));
}
