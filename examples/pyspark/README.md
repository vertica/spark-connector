# Pyspark Example

This example show how to configure a PySpark application with our connector.

In general, you would want to define the appropriate connector options. Then, include the connector's fat jar into
`spark-submit` argument `--jars`, For example:
```
spark-submit --master local[*] --jars <path-to-connector-fat-jar> example.py
```

# How to Run the Example

Make sure you have Docker client installed and running.
First, set up the docker environment as mentioned in [examples](/examples/README.md), then:

1. Download the spark connector "all" jar from our [releases](https://github.com/vertica/spark-connector/releases) 
and place it in to `/connector/target/scala-2.12/`. You can do this on your local machine as this folder is mounted.  
Alternatively, you could build the jar yourself by following the instructions [here](/CONTRIBUTING.md).
2. Assuming you are in the client container, use `cd /spark-connector/examples/pyspark` then run the `./run-python-example.sh` script.
This will submit the pyspark example to our [standalone cluster](localhost:8080).
3. To shut down, exit out of the container with `exit`. Then on your local machine navigate to `spark-connector/docker`
and tear down containers by running `docker-compose down`.

# Other Connector Options
For examples of other options, refer to our [scala example](/examples/scala) which demonstrate how to configure the different
connector options. While it is in a different language, the ideas are transferable; set the correct options, include our connector jar, then spark-submit.