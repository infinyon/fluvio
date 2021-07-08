const path = require("path");
const core = require("@actions/core");
const github = require("@actions/github");
const artifact = require("@actions/artifact");

const runOnce = async () => {
  const artifactClient = artifact.create();

  // Load input and environment variables
  const artifactInput = core.getInput('artifact', { required: true }).trim();
  const artifactNames = artifactInput.split("\n");
  const target = core.getInput('target').trim();
  const sha = process.env.GITHUB_SHA;
  const ref = process.env.GITHUB_REF;
  const repoPath = path.normalize(`${__dirname}/../../..`);
  const releaseMode = github.context.eventName === "push" && ref === "refs/heads/staging";

  // Assemble path into `target/` directory for each artifact based on target and release
  const buildPrefix = (!!target) ? `target/${target}` : "target";
  const buildRelease = (releaseMode) ? "release" : "debug";
  const targetPath = `${buildPrefix}/${buildRelease}`;

  core.info(`artifact: ${artifact}`);
  core.info(`artifacts: ${JSON.stringify(artifactNames)}`);
  core.info(`target: ${target}`);
  core.info(`targetPath: ${targetPath}`);
  core.info(`sha: ${sha}`);
  core.info(`ref: ${ref}`);
  core.info(`release: ${releaseMode}`);
  core.info(`buildPrefix: ${buildPrefix}`);
  core.info(`buildRelease: ${buildRelease}`);
  core.info(`__dirname: ${__dirname}`);
  core.info(`repoPath: ${repoPath}`);

  for (let i = 0; i < artifactNames.length; i++) {
    const artifactName = artifactNames[i];
    // E.g. fluvio-x86_64-unknown-linux-musl
    const artifactKey = `${artifactName}-${target}`;

    // E.g. /home/user/fluvio/fluvio/target/x86_64-unknown-linux-musl/release
    const artifactRoot = path.resolve(repoPath, targetPath);
    const artifactPath = `${artifactRoot}/${artifactName}`;
    const options = { continueOnError: false };
    await artifactClient.uploadArtifact(artifactKey, [artifactPath], artifactRoot, options);
    core.info(`Uploaded ${artifactKey} from ${artifactRoot}`);
  }
};

const run = async () => {
  const retries = 10;
  for (let i = 0; i < retries; i++) {
    try {
      await runOnce();
      break;
    } catch (e) {
      if (i === retries - 1)
        throw e;
      logError(e);
      console.log("RETRYING after 10s");
      await sleep(10000)
    }
  }
}

const sleep = async (millis) => new Promise(resolve => setTimeout(resolve, millis));

const logError = (e) => {
  console.log("ERROR: ", e.message);
  try {
    console.log(JSON.stringify(e, null, 2));
  } catch (e) {
    // We tried
  }
  console.log(e.stack);
}

run().catch(err => {
  logError(err);
  core.setFailed(err.message);
})
