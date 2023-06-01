# Contributing

We appreciate the interest in contributing to the Vertica Spark Connector. This document lays out the contribution process.

There are a number of ways you can help:
- [Bug Reports](#bug-reports)
- [Feature Requests](#feature-requests)
- [Code Contributions](#code-contributions)

## Bug Reports

If you find a bug, submit an [issue](https://github.com/vertica/spark-connector/issues) with a complete and reproducible bug report. If the issue can't be reproduced, it will be closed. If you opened an issue, but figured out the answer later on your own, please comment on the issue to let people know, then close the issue.

For issues (e.g. security related issues) that are **not suitable** to be reported publicly on the GitHub issue system, report your issues to [Vertica open source team](mailto:vertica-opensrc@microfocus.com) directly or file a case with Vertica support if you have a support account.

## Feature Requests

Feel free to share your ideas for how to improve the connector. You can open an [issue](https://github.com/vertica/spark-connector/issues) with details describing what feature(s) you'd like to be added or changed.

If you would like to implement the feature yourself, open an issue to ask before working on it. Once approved, please refer to the [Code Contributions](#code-contributions) section.

## Code Contributions

### Step 1: Install the prerequisites

Install the following prerequisites on your machine:
- Docker Desktop
- IDE (such as VS Code or IntelliJ)
- JDK 8 or 11
- sbt (will manage Scala automatically)

Technically the JDK and sbt do not need to be installed since they will be available in the client Docker container, but since sbt builds run very slow on shared volumes it is recommended to do the actual building on your host machine.

Note the use of JDK 8 or 11 as the version of Scala we currently use (2.12.12) does not work with newer versions of the JDK (see [here](https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html)).

### Step 2: Fork the project

Fork the project from the [GitHub repo](https://github.com/vertica/spark-connector) and checkout your local environment.

```shell
git clone git@github.com:<USERNAME>/spark-connector.git
cd spark-connector
```

Your GitHub repository `<USERNAME>/spark-connector` will be called `origin` in
Git. You should also setup `vertica/spark-connector` as an `upstream` remote.

```shell
git remote add upstream git@github.com:vertica/spark-connector.git
git fetch upstream
```

If you get a `git@github.com: Permission denied (publickey)` error, please follow [this guide](https://stackoverflow.com/questions/58429764/git-could-not-read-from-remote-repository) to setup an SSH key.

#### Configure Git for the first time

Make sure git knows your [name](https://help.github.com/articles/setting-your-username-in-git/) and [email address](https://help.github.com/articles/setting-your-commit-email-address-in-git/):

```shell
git config --global user.name "John Smith"
git config --global user.email "email@example.com"
```

### Step 3: Branch

Create a new branch for the work with a descriptive name:
```shell
git checkout -b my-fix-branch
```

### Step 4: Build the connector

Our standard way of distributing the connector is an assembly JAR. This JAR contains all the dependencies of the connector. To build this JAR, we use the build tool SBT.

See instructions for downloading SBT [here](https://www.scala-sbt.org/download.html).

Use this tool to build the connector JAR:
```shell
cd connector
sbt assembly
```

Running this will run all unit tests and build the JAR to `target/scala-2.12/spark-vertica-connector-assembly-<VERSION>.jar`.

**Note:** Make sure that you are building the JAR on your local machine; building it inside our docker environment will be extremely slow due to Docker shared volumes being slow.

### Step 5: Setup an environment

The following services are required:
- Vertica
- HDFS (or another intermediary filestore, like S3 or GCS)
- Spark (either run locally or a Spark cluster)

Follow our [Docker guide](docker/README.md) to create all of the necessary Docker containers, including a single-node cluster of Vertica, HDFS, and Spark.  Additional containers used for development are also created, such as a client container with all of the necessary prerequisites for building and running the Spark Connector.

Once the containers are running you can run a Spark application that makes use of the Spark Connector.  See the [examples](/examples) or [functional-tests](/functional-tests) folders for example applications that can be run.

#### Vertica

If using your own Vertica instance, refer to the [Vertica Installation Guide](https://www.vertica.com/docs/latest/HTML/Content/Authoring/InstallationGuide/Other/InstallationGuide.htm). Our integration tests run on a single-node Vertica server and that should be all that's required for development unless you are working on a feature specifically relating to multi-node functionality, or are testing performance.

#### AWS S3 or Google GCS

As an alternative to using HDFS, follow our [S3 user guide](docs/s3-guide.md) or the [GCS user guide](docs/gcs-guide.md) to use one of those filesystems instead.

### Step 6: Implement your fix or feature

At this point, you're ready to make your changes. Feel free to reach out over GitHub for help navigating the code and implementing the fix or feature.

#### Debugging in Docker

The following example shows how to debug the functional-tests.

First submit the functional-tests in debug mode:
```shell
docker exec -it docker-client-1 bash
cd /spark-connector/functional-tests
# This will start listening on port 5005 and will not continue until a connection from the debugger is made
# Specify the suite and test parameters to run a single test
./submit-functional-tests-debug.sh --suite EndToEndTests --test "read data from Vertica"
```

The application will now wait for the IDE debugger to connect.  Follow the instructions for your particular IDE and connect the debugger to `localhost:5005`.  For example, see remote debugging of Java applications for [IntelliJ](https://www.jetbrains.com/help/idea/tutorial-remote-debug.html) or [VS Code](https://code.visualstudio.com/docs/java/java-debugging).

Note that this can also be done through sbt.  In that case run `sbt -jvm-debug localhost:5005`, then connect with your debugger before executing your `run` command in sbt.

#### Connector Architecture

The connector has a core architecture, which is called by an implementation of the Spark DataSource V2 API.

![V2 DataSource Implementation](img/SparkInterfaces.png?raw=true "V2 DataSource Implementation")

![Core Architecture](img/CoreArchitecture.png?raw=true "Core Architecture")

This is setup to allow for future work implementing different transport methods between Spark and Vertica. The pipe interface is the piece that could be swapped out. Right now, the only implementation is one that uses a distributed filestore as an intermediary.

DSConfigSetup is where parsing and validation of user-provided configuration happens. The DSReader/DSWriter contain logic for reading/writing that's agnostic to the type of pipe implemented. Finally, we have the bottom-layer components responsible for IO to interfaces such as HDFS and JDBC.

#### Scala Style / Best Practices

We stick to scalastyle for keeping our code in line with good Scala style. This is checked in the CI process, and this can be checked locally with `sbt scalastyle` or a plugin for your IDE.

We have some best practices for Scala development that we try to follow with this connector:
- Prefer immutable data to mutable data. That means preferring `val` over `var`. The former is similar to `final` in Java in that the variable binding is immutable, but the value itself can be mutated if you call a method that mutates it. Also note that there is a `final` in Scala too, which declares that a member may not be overridden in subclasses, so final val is valid in Scala. Scala also has great support for immutable data structures. The reason for preferring immutability is that it makes reasoning with code simpler and also makes code more thread-safe.
- Prefer a more declarative style. Usually this just means using maps, filters, and folds. For-comprehensions also fall under this though, since they are Scala’s equivalent to monadic do-notation. Arguably, this is fairly subjective, but the Scala community tends to find this style more readable.
- Avoid implicits. There are very few cases where they are useful (emulating type-classes is one), but in most OO code they are completely unnecessary and only make code much harder to follow and reason with.
- Use `Option[T]` instead of using null. `Option[T]` can be checked by the compiler to prevent NullPointerExceptions from happening at runtime.
- As an aside, null exists purely for interop with Java.
- When writing recursive functions, use tail recursion and add the `@tailrec` annotation to ensure that it is properly tail-call optimized by the compiler.
- If you are creating newtype wrapper classes, you should extend AnyVal to avoid boxing (allocating runtime objects), as described [here](https://docs.scala-lang.org/overviews/core/value-classes.html).
- Avoid exceptions, use `Either[ErrorType, ReturnType]` where possible for error handling.

#### License Headers

Every file in this project must use the following Apache 2.0 header (with the appropriate year or years in the `[yyyy]` box; if a copyright statement from another party is already present in the code, you may add the statement on top of the existing copyright statement):

```
Copyright (c) [yyyy] Micro Focus or one of its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

#### Commits

Make some changes on your branch, then stage and commit as often as necessary:
```shell
git add .
git commit -m "Added two more tests for #166"
```

When writing the commit message, try to describe precisely what the commit does. The commit message should be within a 72 character maximum. Include the issue number `#N`, if the commit is related to an issue.

#### Tests

We have two suites of tests that you should care about when making changes to the connector. Unit tests, found under the connector folder, and integrations tests found in the functional-tests folder. Each pull request with code changes should include changes to at least one of these.

##### Unit Tests

For changes to the connector where they are possible and practical, unit tests should be included. The connector is composed of components with interfaces, so that a component can be tested with its dependencies mocked. See existing tests for many examples of this.

To see if tests are working, you may run:
```shell
sbt test
```

Note that `sbt assembly` also calls `sbt test`. The tests may also be run from an IDE, such as IntelliJ.

We try to keep overall unit test coverage above a certain threshold. To see if the connector is meeting that threshold, run:

```shell
sbt coverage test coverageReport
```

This step will fail if the coverage level falls under the threshold.

##### Integration Tests

The functional-tests folder is meant to run against a real environment. See [Setup an Environment](#step-4-setup-an-environment) above. To configure the tests to connect to an environment, enter the details in `src/main/resources/application.conf`.

This project contains a series of end-to-end tests. It also contains tests for components on the bottom layer of the connector, those that do some kind of IO directly, and thus don't make as much sense to unit test.

If you set the `sparkVersion` in `build.sbt` to `3.0.0`, you will also need to use `hadoop-hdfs` version `2.7.0` when running `sbt run` to run the integration tests.

Similarly, if you set the `sparkVersion` in `build.sbt` to `3.2.0`, you will also need to use `hadoop-hdfs` version `3.3.0`.

If a change is made to one of those bottom-layer components (e.g. VerticaJdbcLayer, FileStoreLayer), integration tests should be included. Additionally, if a change is large and touches many components of the connector, integration tests should be included.

These integration tests are run automatically via GitHub Actions when a PR is created or updated (and again when the PR is merged to `main`).

### Step 7: Push and Rebase

You can publish your work on GitHub just by doing:
```shell
git push origin my-fix-branch
```

When you go to your GitHub page, you will notice commits made on your local branch is pushed to the remote repository.

When upstream (`vertica/spark-connector`) has changed, you should rebase your work. The `git rebase` command creates a linear history by moving your local commits onto the tip of the upstream commits.

You can rebase your branch locally and force-push to your GitHub repository by doing:
```shell
git checkout my-fix-branch
git fetch upstream
git rebase upstream/master
git push -f origin my-fix-branch
```

### Step 8: Make a Pull Request

When you think your work is ready to be pulled into the Spark Connector, you should create a pull request (PR) at GitHub.

A good pull request means:
- commits with one logical change in each
- well-formed messages for each commit
- documentation and tests, if needed

Go to https://github.com/YOURUSERNAME/spark-connector and [make a Pull Request](https://help.github.com/articles/creating-a-pull-request/) to `vertica:main`.

#### Sign the CLA

Before we can accept a pull request, we first ask people to sign a Contributor License Agreement (or CLA). We ask this so that we know that contributors have the right to donate the code. You should notice a comment from **CLAassistant** on your pull request page, follow this comment to sign the CLA electronically.

#### Review

Pull requests are usually reviewed within a few days. If there are comments to address, apply your changes in new commits, rebase your branch and force-push to the same branch, re-run unit and integration suites to ensure tests are still passing. In order to produce a clean commit history, our maintainers would do squash merging once your PR is approved, which means combining all commits of your PR into a single commit in the master branch.

That's it! Thank you for your code contribution!

After your pull request is merged, you can safely delete your branch and pull the changes from the upstream repository.

### Step 9: Publish a Release

Once all of the features and bugs for a given release are complete, follow this release checklist:
1. Tag issues with a milestone to make listing the changes in a release easier, otherwise look at list of closed PRs since last release
2. Branch and create a PR for next release by changing the version number under `CONTRIBUTING.md`, `version.properties`, and other files (search for the previous release version in the codebase)
3. Create a new draft release, listing the features added and bugs fixed (create a new tag for `vx.y.z` when publishing the release, such as `v3.2.1`)
4. Merge the version update PR
5. Ensure local code is up-to-date, then build the JARs (`sbt clean package` named `spark-vertica-connector-slim-x.y.z.jar`, and `sbt clean assembly` named `spark-vertica-connector-all-x.y.z.jar`), and attach to the release
6. If the GitHub Actions tests pass, publish the release
7. Ask [Vertica open source team](mailto:vertica-opensrc@microfocus.com) to upload the JARs to Maven (takes a few hours for the JARs to show up on [Maven Central](https://mvnrepository.com/artifact/com.vertica.spark/vertica-spark))

#### Versioning

We are targeting support for all Spark releases since 3.0. Our release number format is `major.minor.patch`, with the major and minor numbers always trying to match the latest Spark release.
