## Complex Type Demo
This complex type demo contains multiple main classes, each showing an example of a complex type. Each example will
write then read complex data to and from Vertica. As such, you will need a Spark-Vertica environment to test these examples. 
Included examples are:
- Native array
- Set
- Complex Array
- Row Type
- External table Map

We provide a [sandbox environment](../README.md#Prepare-test-environment) that you can use run these examples.

Assuming you are using the sandbox environment:
- Navigate to inside `docker_client_1`, navigate to this folder.
- Use `sbt run`. You will be prompted with a list of example main class to execute.
