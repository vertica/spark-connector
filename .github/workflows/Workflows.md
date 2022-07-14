# GitHub Work Flows
The following are descriptions of workflows used in the repository.

### On Pull Request
The workflow `on-pull-request.yml` runs on a pull requests to `main` (when the PR is created or has content pushed to it).
THe purpose is to perform the most critical tests required for merging. These tests should be set as required status checks 
thus requiring the PR to pass them before being merged. 

Currently, this includes:
* Compile checks
* Unit-tests checks with coverage result uploaded to CodeCov
* Scalastyle checks
* Integration tests against the latest Spark 3 and Vertica

### Nightly Testing
The workflow `nightly.yml` runs nightly during weekdays, executing `main` branch against non-critical tests. The workflow is used for
to perform regression testings and functional testing against different environments.

Currently, tests include:
* Integration tests against:
  * GCS
  * S3
* Testing all combinations of:
  * Spark 3.0 to latest
  * Vertica 10.1.1-0, 11.1.1-2, and 12.0.0-0



