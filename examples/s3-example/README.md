# How to run this example

Make sure you have docker and sbt installed. Tested using docker 20.10.0, sbt 1.4.1

Clone this repository:
```
git clone https://github.com/vertica/spark-connector.git
```

In the directory for this example (the one containing this README), create a lib directory:
```
mkdir lib
```

Put the assembly jar for the connector in the lib folder you just created. If you don't have the jar file, you can [download it here](https://github.com/vertica/spark-connector/releases).

From the project's root directory:
```
cd docker
docker compose up -d
```

Run `docker exec -it docker_client_1 /bin/bash` to enter the sandbox client environment. The following steps assume you are in this container.

Change directory to the example project and run `sbt assembly`:
```
cd /spark-connector/examples/s3-example/
sbt assembly
```

Set up a Spark cluster locally in the sandbox client container, as described in [our S3 user manual](https://github.com/vertica/spark-connector/blob/main/S3UserManual.md).

Set the JAVA_HOME and SPARK_HOME environment variables and add the bin and sbin folders to PATH. Then, start up Spark:
```
export JAVA_HOME=/usr/lib/jvm/jre-11-openjdk
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
start-master.sh
start-slave.sh spark://localhost:7077
```

In src/main/resources/application.conf, you must update the filepath to use your S3 bucket, as follows:
`filepath="s3a://<your S3 bucket name here>/"`

# Configuring Authentication

## Access Key ID + Secret Access Key
Set the `aws_access_key_id` and `aws_secret_access_key` options in src/main/resources/application.conf by adding these two lines:
```
aws_access_key_id="<your access key id>"
aws_secret_access_key="<your secret access key>"
```

## IAM Roles

If you have configured IAM roles for authentication, you need to add this line to src/main/resources/application.conf:
`aws_credentials_provider="org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"`

# Configuring 2FA

If you have two-factor authentication enabled, you need to set your session token by adding this line to src/main/resources/application.conf:
`aws_session_token="<your session token>"`

# Configuring the AWS region for your bucket

Set the `aws_region` option in src/main/resources/application.conf by adding this line:
`aws_region="<your region>"`

# Running the example

You should now be able to use spark-submit to run the example:
`spark-submit --master spark://localhost:7077 target/scala-2.12/spark-vertica-connector-functional-tests-assembly-1.0.jar`
