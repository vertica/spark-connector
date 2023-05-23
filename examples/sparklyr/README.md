# Sparklyr Example

The connector can be used with R by using the sparklyr library. 

In general, you would want to include the connector's fat JAR into Spark's config, then define the appropriate connector options into the option list for a read or write.

# How to run the example

First, set up the Docker environment as mentioned in [examples](/examples/README.md), then:
1. Download the spark connector "all" jar from our [releases](https://github.com/vertica/spark-connector/releases) and place it in to `/connector/target/scala-2.12/`. You can do this on your local machine as this folder is mounted. Alternatively, you could build the jar yourself by following the instructions [here](/CONTRIBUTING.md)
2. Assuming you are in the client container, use `cd /spark-connector/examples/sparklyr` then run the `./run-r-example.sh` script. This will install R and necessary packages before starting the r script. You can see the submitted app on our [standalone cluster](localhost:8080)
3. To shut down, exit out of the container with `exit`. Then on your local machine navigate to `spark-connector/docker` and tear down containers by running `docker-compose down`

# Other Connector Options

For examples of other options, refer to our [Scala example](/examples/scala) which demonstrate how to configure the different connector options. While it is in a different language, the ideas are transferable; set the correct options, include our connector JAR, then spark-submit.
