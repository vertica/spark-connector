# How to run this example

Make sure you have docker and sbt installed, and that Docker client is running. Tested using docker 20.10.0, sbt 1.4.1

First, clone the connector repository as mentioned in [examples](/examples/README.md).

Change directory to the example project:

```
cd /spark-connector/examples/s3-example
```

# S3 Configuration

If you are using a local Minio instance in place of S3, keep the filepath and other AWS options in `src/main/resources/application.conf` as is.

If you are using S3, update the options in `src/main/resources/application.conf` as described below.

## S3 Server + Bucket

You must update the filepath to use your S3 bucket, as follows:
```
filepath="s3a://<your S3 bucket name here>/"
```

Also remove the `aws_endpoint`, `aws_enable_ssl`, and `aws_enable_path_style` options entirely.  AWS uses a default endpoint (`s3.amazonaws.com`), and always uses SSL and virtual-host style access (as opposed to path style access).

## Access Key ID + Secret Access Key

Set the `aws_access_key_id` and `aws_secret_access_key` options by adding these two lines:
```
aws_access_key_id="<your access key id>"
aws_secret_access_key="<your secret access key>"
```

## IAM Roles

If you have configured IAM roles for authentication, you need to add this line:
```
aws_credentials_provider="org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
```

## Configuring 2FA

If you have two-factor authentication enabled, you need to set your session token by adding this line:
```
aws_session_token="<your session token>"
```

## Configuring the AWS region for your bucket

Set the `aws_region` option by adding this line:
```
aws_region="<your region>"
```

# Running the example

## Prepare test environment

Change directory to `/spark-connector/docker` and run:

```
./sandbox-clientenv.sh
```
On Windows, you can run the equivalent batch file:

```
sandbox-clientenv.bat
```

This will put you in the sandbox client (i.e. client container) environment.

## Build Connector Assembly
Change directory to the `connector` folder of the project:
```
cd /spark-connector/connector
```

Build the connector's assembly jar:
```
sbt assembly
```

## Add Connector Assembly as a Dependency to the Example Project
Create a `lib` folder under the `s3-example` folder and copy the assembled connector jar to it.
```
mkdir /spark-connector/examples/s3-example/lib
cp /spark-connector/connector/target/scala-2.12/spark-vertica-connector-assembly-3.0.1.jar /spark-connector/examples/s3-example/lib
```

## Build Assembly

In the project's root directory `spark-connector/examples/s3-example` run:

```
sbt assembly
```

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