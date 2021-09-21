# Pyspark Example

The connector can be used with pyspark, it must simply be sourced as a JAR file.

An example python application is provided alongside a .sh file which shows how to run such an application with the connector. This script will download Python3, Spark, and Hadoop and configure them before running the example.

# How to run the example


Make sure you have Docker and SBT installed, and that Docker client is running. Tested using docker 3.3.1 and SBT 1.4.1.

First, clone the connector repository as mentioned in [examples](/examples/README.md), the run the following steps:

1. Run `sbt assembly` from the `/spark-connector/connector` folder of the repository.
2. Run the `sandbox-clientenv.sh` script in the Docker folder, or `sandbox-clientenv.bat` if running on Windows, which will put you in the sandbox client container.
3. From the sandbox client container, go to this directory (`cd /spark-connector/examples/pyspark`) and run the `./run-python-example.sh` script.
4. Exit out of the interactive terminal by running `exit`. 
5. From the docker folder tear down containers by running `docker-compose down`.

