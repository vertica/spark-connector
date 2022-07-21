# S3 Setup User Guide

Apache Hadoop provides an aws [connector](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) 
allowing Hadoop file system to access S3, and subsequently allowing the connector to use S3 as the staging area.

# Required Dependencies
What you will need:
- Spark 3.x
- An appropriate `hadoop-aws` version for your hadoop install. Note that Spark comes bundled with Hadoop or stand-alone.
  - Importantly, the versions of hadoop-aws must be identical to the hadoop install. 
  - For example, for a sbt project using Hadoop 3.3.0, add to your `build.sbt`:
    ```libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.0"```
- An S3 bucket configured to use either A) access key ID + secret access key or B) IAM roles for authentication

Some features may work with older versions of hadoop-aws, but we currently only test against the hadoop-aws version compatible with
the latest Spark 3.

# Spark with User-provided hadoop.
The following example sets up a **user-provided Apache Hadoop**. To download Spark, [go here](https://spark.apache.org/downloads.html). Be sure to select package type "Pre-built with user-provided Apache Hadoop".

You can [download Hadoop 3.3 here](https://hadoop.apache.org/releases.html). Make sure to download the binary.

## Setting up Spark with Hadoop
Note: All instructions here are for MacOS or Linux users.

First, you will need to decompress the Spark tar file and Hadoop tar file:
```
tar xvf spark-3.0.2-bin-without-hadoop.tgz
tar xvf hadoop-3.3.0.tar.gz
```

Move the resulting folder to /opt/spark/:
`mv spark-3.0.2-bin-without-hadoop/ /opt/spark`

Go to the Spark configuration directory:
`cd /opt/spark/conf`

There should be a spark-env.sh.template file. You will want a real spark-env.sh file, so rename the template to spark-env.sh:
`mv spark-env.sh.template spark-env.sh`

Next, set the JAVA_HOME environment variable:
`export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk`

Now, edit spark-env.sh and point SPARK_DIST_CLASSPATH to the Hadoop folder you extracted earlier. For example, if you extracted it to /myhadoop, you should add the following line:
`export SPARK_DIST_CLASSPATH=$(/myhadoop/hadoop-3.3.0/bin/hadoop classpath)`
See [Spark's documentation](http://spark.apache.org/docs/latest/hadoop-provided.html) for more information.

Finally, set the SPARK_HOME environment variable:
```
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
## Example application using S3
[See here for an example of how to connect to an S3 bucket with this connector](https://github.com/vertica/spark-connector/tree/main/examples/s3-example).

# Troubleshooting
If you see this error:
```java.lang.NoClassDefFoundError: org/apache/hadoop/fs/StreamCapabilities```
it is like because you are not using Spark with Hadoop 3.3.0 and hadoop-aws 3.3.0.