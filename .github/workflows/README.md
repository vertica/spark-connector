# GitHub Work Flows
The following are descriptions of the workflows used in the repository.

### Main Tests
The workflow `main.yml` runs on a pull requests to `main` (when a PR is created or has content pushed to it) or on pushes
to `main` (like when a PR is merged).
The purpose is to perform critical tests required for merging. 

Currently, this includes:
* Compile checks
* Unit-tests checks
* Test coverage checks:
  * Require at least 80% coverage when the PR is merged
  * [Patch coverage](https://docs.codecov.com/docs/commit-status#patch-status) of at least 80%. Patch coverage only measures the coverage of changes made in the PR 
* Scalastyle checks
* Integration tests against the latest Vertica. Uses the default Spark and Hadoop from the functional test which should be the latest.

### Nightly Tests
The workflow `nightly.yml` runs nightly, from Monday to Friday at 9:18 AM GMT (or 2:18 AM Pacific Time), executing the 
`main` branch against non-critical tests. It currently performs regression testing on combinations of Spark 3.x, with 
the appropriate Hadoop HDFS, against Vertica 11.1.1-2 and 12.0.0-0. We also test against the latest Spark 3.x on a 
standalone Spark cluster.

### Weekly Tests
`weekly.yml` performs weekly tests every Monday at 10:18 AM GMT (or 3:18 AM Pacific Time), executing the following tests:
* Integration tests against different intermediary file-store:
  * S3, using a MINIO object store container to mimic S3
  * GCS, against an actual GCS bucket provided by Vertica. We could not find a solution to mock a GCS environment yet
* Testing the `json` option against Spark 3.x
* Test against Vertica 10.1.1-0

Unless specified, all tests use the latest Vertica docker image. This would notify us of breaking changes

### Cleanup Artifacts
`cleanup-artifacts.yml` workflows runs every day at 1AM, removing any artifacts created during a workflow.

### Auto Triage and Remove Issue
When an issue is labeled with a priority, `auto-triage.yml` workflow move it to the backlog, into the respective 
priority column.

`remove-issue.yml` workflow triggers when an issue is closed, removing it from the backlog.





