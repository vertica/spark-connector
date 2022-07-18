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
  * Require at least 80% coverage when the PR is merged.
  * Patch coverage of at least 80%. Patch coverage only measures the coverage of changes made in the PR. 
* Scalastyle checks
* Integration tests against the latest Spark 3 and Vertica

### Nightly Testing
The workflow `nightly.yml` runs nightly, from Monday to Friday at 9:18 AM GMT (or 2:18 AM Pacific Time), executing the 
`main` branch against non-critical tests. It currently performs regression testings and functional testing against different environments.

Currently, testing includes:
* Integration tests against: 
  * S3, using a MINIO object store container to mimic S3.
  * GCS, against an actual GCS bucket provided by Vertica. We could not find a solution to mock a GCS environment yet.
* Testing combinations of:
  * Spark 3.x
  * Vertica 10.1.1-0, 11.1.1-2, and 12.0.0-0
* Testing JSON option against Spark 3.x

### Cleanup Artifacts
`cleanup-artifacts.yml` workflows runs every day at 1AM, removing any artifacts created during a workflow.

### Auto Triage and Remove Issue
When an issue is labeled with a priority, `auto-triage.yml` workflow move it to the backlog, into the respective 
priority column.

`remove-issue.yml` workflow triggers when an issue is closed, removing it from the backlog.





