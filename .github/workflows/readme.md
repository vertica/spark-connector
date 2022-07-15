# GitHub Work Flows
The following are descriptions of the workflows used in the repository.

### On Pull Request
The workflow `on-pull-request.yml` runs on a pull requests to `main` (when a PR is created or has content pushed to it).
The purpose is to perform critical tests required for merging and these tests should be set as required status checks, 
requiring the PR to pass them before being merged. 

Currently, this includes:
* Compile checks
* Unit-tests checks
* Test coverage checks:
  * Require at least 80% coverage when the PR is merged
  * ???
* Scalastyle checks
* Integration tests against the latest Spark 3 and Vertica

### Nightly Testing
The workflow `nightly.yml` runs nightly during weekdays, executing the `main` branch against non-critical tests. It currently is used for
to perform regression testings and functional testing against different environments.

Currently, tests include:
* Integration tests against:
  * GCS
  * S3
* Testing combinations of:
  * Spark 3.x
  * Vertica 10.1.1-0, 11.1.1-2, and 12.0.0-0
* Testing JSON read against Spark 3.x



