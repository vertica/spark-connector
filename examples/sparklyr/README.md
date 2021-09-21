# Sparklyr Example

The connector can be used with R by using the sparklyr library. The connector must simply be sourced as a JAR file. 

# How to run the example

Make sure you have Docker and SBT installed, and that Docker client is running. Tested using docker 3.3.1 and SBT 1.4.1.

First, clone the connector repository as mentioned in [examples](/examples/README.md), the run the following steps:

1. Run `sbt assembly` from the `/spark-connector/connector` folder of the repository.
2. Run the `sandbox-clientenv.sh` script in the Docker folder, or `sandbox-clientenv.bat` if running on Windows, which will put you in the sandbox client container.
3. From the sandbox client container, go to this directory (`cd /spark-connector/examples/sparklyr`) and run the `./run-r-example.sh` script.
4. Exit out of the interactive terminal by running `exit`. 
5. From the docker folder tear down containers by running `docker-compose down`.