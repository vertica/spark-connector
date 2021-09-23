# External Table Example

Since this feature was implemented after our latest release, users will need to do some setup prior to running the example:

1. Clone the connector repository, if not done already:

    `git clone https://github.com/vertica/spark-connector.git`
   

2. Build the connector jar and place into lib folder of example:

    ```
    cd spark-connector/examples/external-table-example &&
    mkdir lib &&
    cd ../../connector &&
    sbt assembly &&
    cp target/scala-2.12/spark-vertica-connector-assembly-2.0.2.jar ../examples/external-table-example lib 
    ```
   
3. Now, run the example like you would any other example mentioned in the [spark-connector/examples README](https://github.com/vertica/spark-connector/blob/main/examples/README.md)