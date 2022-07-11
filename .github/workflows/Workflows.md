# GitHub Work Flows
The following are descriptions of workflows used in the repository.

### OnPullRequest
Workflow runs on a pull request to `main` is created, or on a push to said pull request's branch. It will start all critical 
tests on code being merged. These tests would be set as required status checks thus requiring the PR to pass them before 
being merged. 

Currently, this includes:
* Compile checks
* Unit-tests checks with coverage result uploaded to CodeCov
* Scalastyle checks
* Integration tests against the latest Spark 3 and Vertica

### NightlyActions
This workflow runs nightly during weekdays, executing `main` branch against non-critical tests. This action is used for
regression testings, as well as functional testing against different environment.

Currently, tests include:
* Integration tests against:
  * GCS
  * S3
* Regression testing all combinations of:
  * Spark 3.0 to latest
  * Vertica 10.1.1-0, 11.1.1-2, and 12.0.0-0\
  * Appropriate Hadoop version for Spark???

# Re-usable Workflows 
We provide workflows for manual testings in the form of re-usable workflows. These workflows are used as part of 



