We appreciate the interest in contributing to the Vertica Spark Connector. This document lays out the contribution process.

There are a number of ways you can help:

 - [Bug Reports](#bug-reports)
 - [Feature Requests](#feature-requests)
 - [Code Contributions](#code-contributions)
 
 
 # Bug Reports

If you find a bug, submit an [issue](https://github.com/vertica/spark-connector/issues) with a complete and reproducible bug report. If the issue can't be reproduced, it will be closed. If you opened an issue, but figured out the answer later on your own, comment on the issue to let people know, then close the issue.

For issues (e.g. security related issues) that are **not suitable** to be reported publicly on the GitHub issue system, report your issues to [Vertica open source team](mailto:vertica-opensrc@microfocus.com) directly or file a case with Vertica support if you have a support account.


# Feature Requests

Feel free to share your ideas for how to improve the connector. You can open an [issue](https://github.com/vertica/spark-connector/issues)
with details describing what feature(s) you'd like to be added or changed.

If you would like to implement the feature yourself, open an issue to ask before working on it. Once approved, please refer to the [Code Contributions](#code-contributions) section.


# Code Contributions

## Step 1: Fork the project

Fork the project from the [Github](https://github.com/vertica/spark-connector) and checkout your local environment.

```shell
git clone git@github.com:YOURUSERNAME/spark-connector.git
cd spark-connector
```

Your GitHub repository **YOURUSERNAME/spark-connector** will be called "origin" in
Git. You should also setup **vertica/spark-connector** as an "upstream" remote.

```shell
git remote add upstream git@github.com:vertica/spark-connector.git
git fetch upstream
```
If you get a `git@github.com: Permission denied (publickey)` error, please follow [this guide](https://stackoverflow.com/questions/58429764/git-could-not-read-from-remote-repository) to set up an SSH key.
### Configure Git for the first time

Make sure git knows your [name](https://help.github.com/articles/setting-your-username-in-git/ "Set commit username in Git") and [email address](https://help.github.com/articles/setting-your-commit-email-address-in-git/ "Set commit email address in Git"):

```shell
git config --global user.name "John Smith"
git config --global user.email "email@example.com"
```

## Step 2: Branch

Create a new branch for the work with a descriptive name:

```shell
git checkout -b my-fix-branch
```

## Step 3: Build the connector

Our standard way of distributing the connector is an assembly jar. This jar contains all the dependencies of the connector. To build this jar, we use the build tool SBT.

See instructions for downloading SBT [here](https://www.scala-sbt.org/download.html).

Use this tool to build the connector jar:
```shell
cd connector
sbt assembly
```

Running this will run all unit tests and build the jar to target/[SCALA_VERSION]/spark-vertica-connector-assembly-1.0.jar 

## Step 4: Set up an environment
The easiest way to set up an environment is to spin up the docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS following [this guide.][https://github.com/vertica/spark-connector/blob/main/examples/README.md]

Alternatively, you may download the requirements below:

### Vertica 
The first requirement for a test environment is [Installing Vertica](https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/InstallationGuide/Other/InstallationGuide.htm?tocpath=Installing%20Vertica%7C_____0). Our integration tests run on a single-node Vertica server and that should be all that's required for development unless you are working on a feature specifically relating to multi-node functionality, or are testing performance.

### Spark
The next requirement is a spark application that uses the connector jar. Example projects can be found under the examples directory. To use the jar built above, copy it into a subdirectory called "lib" at the root of the project.

```shell
cd examples/basic-read
mkdir lib
cp ../../connector/target/scala-2.12/spark-vertica-connector-assembly-1.0.jar lib
sbt run
```

### HDFS
<a name="hdfs"></a>
The final requirement is an intermediary distributed filesystem such as HDFS. This acts as an intermediary transport method between Spark and Vertica.

The easiest setup for this in a development environment is to use a docker container with HDFS. We recommend [docker-hdfs](https://github.com/mdouchement/docker-hdfs). You can clone it and then set it up as follows:

```shell
cd docker-hdfs
sudo cp etc/hadoop/* /etc/hadoop/conf
docker build -t mdouchement/docker-hdfs .
docker run -p 22022:22 -p 8020:8020 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -it mdouchement/hdfs
```

The sudo copy step above copies the hadoop configuration to a location where Vertica can pick it up. If Vertica was on a different machine, the configuration files would have to be copied to that machine. 

After spinning up the docker container, it will put you in a shell, where you can manage the hdfs filesystem with commands like 

```shell
hadoop fs -mkdir data
```

A final step may be required for the networking between Vertica and the HDFS docker container to function. Use docker ps to find the container ID, and add an entry to /etc/hosts resolving that ID as a hostname for localhost

```shell
127.0.0.1 <docker-id>
```

This is assuming a single-node Vertica installation on the same machine as the hdfs container.


## Step 5: Implement your fix or feature

At this point, you're ready to make your changes. Feel free to reach out over github for help navigating the code and implementing the fix or feature.

### Connector Architecture

The connector has a core architecture, which is called by an implementation of the Spark Datasource V2 API.

![V2 Datasource Implementation](img/SparkInterfaces.png?raw=true "V2 Datasource Implmentation")

![Core Architecture](img/CoreArchitecture.png?raw=true "Core Architecture")

This is set up to allow for future work implementing different transport methods between Spark and Vertica. The pipe interface is the piece that could be swapped out. Right now, the only implementation is one that uses a distributed filestore as an intermediary.

DSConfigSetup is where parsing and validation of user-provided configuration happens. The DSReader/DSWriter contain logic for reading/writing that's agnostic to the type of pipe implemented. Finally, we have the bottom-layer components repsonsible for IO to interfaces such as HDFS and JDBC.


### Scala Style / Best Practices

We stick to scalastyle for keeping our code in line with good scala style. This is checked in the CI process, and this can be checked locally with "sbt scalastyle" or a plugin for your IDE.

We have some best practices for scala development that we try to follow with this connector:
- Prefer immutable data to mutable data. That means preferring val over var. The former is similar to final in Java in that the variable binding is immutable, but the value itself can be mutated if you call a method that mutates it. Also note that there is a final in Scala too, which declares that a member may not be overridden in subclasses, so final val is valid in Scala. Scala also has great support for immutable data structures. The reason for preferring immutability is that it makes reasoning with code simpler and also makes code more thread-safe.
- Prefer a more declarative style. Usually this just means using maps, filters, and folds. For-comprehensions also fall under this though, since they are Scala’s equivalent to monadic do-notation. Arguably, this is fairly subjective, but the Scala community tends to find this style more readable.
- Avoid implicits. There are very few cases where they are useful (emulating type-classes is one) but in most OO code, they are completely unnecessary and only make code much harder to follow and reason with.
- Use Option[T] instead of using null. Option[T] can be checked by the compiler to prevent NullPointerExceptions from happening at runtime.
- As an aside, null exists purely for interop with Java.
- When writing recursive functions, use tail recursion and add the @tailrec annotation to ensure that it is properly tail-call optimized by the compiler.
- If you are creating newtype wrapper classes, you should extend AnyVal to avoid boxing (allocating runtime objects), as described here: https://docs.scala-lang.org/overviews/core/value-classes.html
- Avoid exceptions, use Either[ErrorType, ReturnType] where possible for error handling.


### License Headers

Every file in this project must use the following Apache 2.0 header (with the appropriate year or years in the "[yyyy]" box; if a copyright statement from another party is already present in the code, you may add the statement on top of the existing copyright statement):

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

### Commits

Make some changes on your branch, then stage and commit as often as necessary:

```shell
git add .
git commit -m 'Added two more tests for #166'
```

When writing the commit message, try to describe precisely what the commit does. The commit message should be within a 72 character maximum. Include the issue number `#N`, if the commit is related to an issue.

### Tests

We have two suites of tests that you should care about when making changes to the connector. Unit tests, found under the connector project, and integrations tests found in the functional-tests project. Each pull request with code changes should include changes to at least one of these.

#### Unit Tests

For changes to the connector where they are possible and practical, unit tests should be included. The connector is composed of components with interfaces, so that a component can be tested with its dependencies mocked. See existing tests for many examples of this. 

To see if tests are working, you may run:
```shell
sbt test
```

The tests may also be run from an IDE such as IntelliJ.

We try to keep overall unit test coverage above a certain threshold. To see if the connector is meeting that threshold, run:

```shell
sbt coverage test coverageReport
```

This step will fail if the coverage level falls under the threshold.

#### Integration Tests

The functional-tests project is meant to run against a real environment. See "Set up an Environment" above. To configure the project to connect to an environment, enter the details in src/main/resources/application.conf 

This project contains a series of end-to-end tests. It also contains tests for components on the bottom layer of the connector, those that do some kind of IO directly, and thus don't make as much sense to unit test.

If a change is made to one of those bottom-layer components (ie VerticaJdbcLayer, FileStoreLayer), integration tests should be included. Additionally, if a change is large and touches many components of the connector, integration tests should be included.


## Step 6: Push and Rebase

You can publish your work on GitHub just by doing:

```shell
git push origin my-fix-branch
```

When you go to your GitHub page, you will notice commits made on your local branch is pushed to the remote repository.

When upstream (vertica/spark-connector) has changed, you should rebase your work. The **rebase** command creates a linear history by moving your local commits onto the tip of the upstream commits.

You can rebase your branch locally and force-push to your GitHub repository by doing:

```shell
git checkout my-fix-branch
git fetch upstream
git rebase upstream/master
git push -f origin my-fix-branch
```

## Step 7: Make a Pull Request

When you think your work is ready to be pulled into the Spark Connector, you should create a pull request(PR) at GitHub.

A good pull request means:
 - commits with one logical change in each
 - well-formed messages for each commit
 - documentation and tests, if needed

Go to https://github.com/YOURUSERNAME/spark-connector and [make a Pull Request](https://help.github.com/articles/creating-a-pull-request/) to `vertica:main`. 

### Sign the CLA

Before we can accept a pull request, we first ask people to sign a Contributor License Agreement (or CLA). We ask this so that we know that contributors have the right to donate the code. You should notice a comment from **CLAassistant** on your pull request page, follow this comment to sign the CLA electronically. 


### Review
Pull requests are usually reviewed within a few days. If there are comments to address, apply your changes in new commits, rebase your branch and force-push to the same branch, re-run unit and integration suites to ensure tests are still passing. In order to produce a clean commit history, our maintainers would do squash merging once your PR is approved, which means combining all commits of your PR into a single commit in the master branch.

That's it! Thank you for your code contribution!

After your pull request is merged, you can safely delete your branch and pull the changes from the upstream repository.

[]: https://stackoverflow.com/questions/58429764/git-could-not-read-from-remote-repository

[]: https://github.com/vertica/spark-connector/blob/main/examples/README.md

[https://github.com/vertica/spark-connector/blob/main/examples/README.md]: https://github.com/vertica/spark-connector/blob/main/examples/README.md