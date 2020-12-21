---
name: Bug report
about: Create a report to help us improve
title: "[Bug]:"
labels: bug
assignees: ''

---

**What happened**
A clear and concise description of what the bug is.

**Expected behavior**
A clear and concise description of what you expected to happen.

**Describe the setup**
- Are you using a local Fluvio install? Minikube? Fluvio Cloud?
- What version of Fluvio are you using? `fluvio version`

**How to reproduce it (as minimally and precisely as possible)**
Steps to reproduce the behavior:
1. Run the command '...'
2. Type the input '...'

**Log output**
It helps to have logs from Fluvio's SC and SPU processes.
Depending on your setup, here's how you can get the logs:

- For a local Fluvio installation on Mac:
  - Run `cat /usr/local/var/log/fluvio/flv_sc.log` for SC logs
  - Run `cat /usr/local/var/log/fluvio/spu_log_XXXX.log` for each SPU
    - E.g. when running 1 SPU, there will be `spu_log_5001.log`
- For a Fluvio installation on Minikube:
  - Run `kubectl logs fluvio-sc` for SC logs
  - Run `kubectl logs fluvio-spg-main-X` for each SPU

**Environment (please complete the following information):**
 - OS: [e.g. Linux, Mac]
 - Fluvio Version [e.g. 22]
 - Minikube version (if used): use `minikube version`
 - Kubernetes version: use `kubectl version`

**Additional context**
Add any other context about the problem here.
