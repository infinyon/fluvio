const core = require("@actions/core");
const github = require("@actions/github");

const runOnce = async () => {
  // Load input and environment variables
  const artifact = core.getInput('artifact', { required: true });
  const target = core.getInput('target');
  const sha = process.env.GITHUB_SHA;
  const ref = process.env.GITHUB_REF;
  const context = github.context;
  const releaseMode = context.eventName === "push" && ref === "refs/heads/staging";

  const buildPrefix = (!!target) ? `target/${target}/` : "target/";
  const buildRelease = (releaseMode) ? "release" : "debug";

  core.info(`artifact: ${artifact}`);
  core.info(`target: ${target}`);
  core.info(`sha: ${sha}`);
  core.info(`ref: ${ref}`);
  core.info(`release: ${releaseMode}`);
  core.info(`buildPrefix: ${buildPrefix}`);
  core.info(`buildRelease: ${buildRelease}`);
  core.info(`context: ${JSON.stringify(context)}`);
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
