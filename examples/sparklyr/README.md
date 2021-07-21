# Sparklyr Example

The connector can be used with R by using the sparklyr library. The connector must simply be sourced as a JAR file. 

# How to run the example

1. Run `sbt assembly` from the connector folder of the repository.
2. Run the `sandbox-clientenv.sh` script in the Docker folder, which will put you in the sandbox client container.
3. From the sandbox client container, go to this directory (`cd /spark-connector/examples/sparklyr`) and run the `run-r-example.sh` script.