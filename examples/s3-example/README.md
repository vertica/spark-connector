# How to run this example

Make sure you have docker and sbt installed, and that Docker client is running. Tested using docker 20.10.0, sbt 1.4.1

First, clone the connector repository as mentioned in [examples](/examples/README.md).

Change directory to the example project:

```
cd spark-connector/examples/s3-example
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
```
aws_credentials_provider="org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
```

# Configuring 2FA

If you have two-factor authentication enabled, you need to set your session token by adding this line to src/main/resources/application.conf:
```
aws_session_token="<your session token>"
```

# Configuring the AWS region for your bucket

Set the `aws_region` option in src/main/resources/application.conf by adding this line:
```
aws_region="<your region>"
```

# Running the example

Following are the same steps as described in [Running Application in Spark Cluster](/examples/README.md#running-application-in-spark-cluster).
## Build Assembly

In the project's root directory `spark-connector/examples/s3-example` run:

```
sbt assembly
```

## Prepare test environment

Change directory to `spark-connector/docker` and run:

```
./sandbox-clientenv.sh
```
On Windows, you can run the equivalent batch file:

```
sandbox-clientenv.bat
```

This will put you in the sandbox client (i.e. client container) environment.

## Run test

The following steps assume you are in the client sandbox environment.

Change your working directory to `spark-connector/examples`, then run:

```
./run-example.sh  s3-example
```

If you get a permission denied issue, run `chmod +x run-example.sh`

## Tear down containers

Exit out of the interactive terminal by running `exit`. 

From the `spark-connector/docker` folder, run the following:

```
docker compose down
```