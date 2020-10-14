---
name: Bug report
about: Create a report to help us improve
title: "[Bug]:"
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**Describe the setup**
- Are you using a local Fluvio install? Minikube? Fluvio Cloud?
- What version of Fluvio are you using? `fluvio version`

**To Reproduce**
Steps to reproduce the behavior:
1. Run the command '...'
2. Type the input '...'

**Expected behavior**
A clear and concise description of what you expected to happen.

**Log output**
It helps to have logs from Fluvio's SC and SPU processes.
Depending on your setup, here's how you can get the logs:

- For a local Fluvio installation on Mac:
  - Run `cat /usr/local/var/log/fluvio/flv_sc.log` for SC logs
  - Run `cat /usr/local/var/log/fluvio/spu_log_XXXX.log` for each SPU
    - E.g. when running 1 SPU, there will be `spu_log_5001.log`
- For a Fluvio installation on Minikube:
  - Run `kubectl logs flv-sc` for SC logs
  - Run `kubectl logs flv-spg-main-X` for each SPU

**Screenshots**
If applicable, add screenshots to help explain your problem.

**Environment (please complete the following information):**
 - OS: [e.g. iOS]
 - Fluvio Version [e.g. 22]

**Additional context**
Add any other context about the problem here.
