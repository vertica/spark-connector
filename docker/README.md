# Docker Environment

We provide a docker environment for testing and developing that contains the following containers:
- `docker-client-1`: The client environment. This is where spark application are submitted/executed from
- `docker-hdfs-1`: The HDFS node acting as the staging file-store
- `docker-vertica-1`: Our Vertica database
- `docker-minio-1`: A minio file-store container acting as an S3 file-store 
- `docker-spark-1`: A Spark driver for our standalone cluster
- `docker-spark-worker-*`: Spark workers for our standalone cluster, created in swarm mode

When using the Kerberos configuration the container names are simplified (`client`, `hdfs`, and `vertica`, plus one for the Kerberos key server, `kdc`).  The Spark and MinIO containers are not currently deployed for Kerberos.

### Starting the Environment

To start, make sure you have Docker and sbt installed, and that Docker client is running.

Then, clone the repository if you haven't already:
```
git clone https://github.com/vertica/spark-connector.git
```

Then use Docker Compose to start the containers:
```
docker-compose up -d
```

Or, for the Kerberos configuration:
```
docker-compose -f docker-compose-kerberos.yml up -d
```

Once started, you can visit the Spark standalone cluster [Master WebUI](localhost:8080) and the first Spark context UI at 
localhost:4040 (won't be available until a context is created).

You can access a container shell as follows:
```
docker exec -it <container-name> bash

# Client container example:
docker exec -it docker-client-1 bash
# or
docker exec -it krb-client bash
```

You can exit the container's shell with `exit`.

Note that it can take a few seconds for some of the services, notably Vertica and HDFS, to initialize.  Before running any tests, ensure that the services are up by looking for the "\<service\> container is now running" message at the end of the logs:
```
docker logs docker-vertica-1
docker logs docker-hdfs-1
```

### Configurations

The following env vars can be exported or specified in your `.env` file:
- `VERTICA_VERSION`: Setting the version of our Vertica database. By default, we use the [latest](https://hub.docker.com/r/vertica/vertica-k8s) Vertica docker image. For example, to use Vertica 10.1.1-0 use `export VERTICA_VERSION=10.1.1-0`.
- `SPARK_INSTALL`: Setting the version of our Spark install. By default, we use the [latest](https://hub.docker.com/r/bitnami/spark) bitnami Spark image. For example, to use Spark 3.1.3 use `export SPARK_INSTALL=3.1.3`.

By default we start a single Spark worker.  To start multiple Spark workers, such as 3 workers:
```
docker-compose up -d --scale spark-worker=3
```

For other configurations parameters, since we are using bitnami as the base image of our cluster and client, 
refer to their [available options](https://hub.docker.com/r/bitnami/spark) and set the appropriate environment variables in `docker-compose.yml` or your `.env` file.

### Tear Down the Environment

Once you're finished running the example, exit out of the interactive terminal by running `exit`.

Make sure you are still in the docker directory, then run:
```
docker-compose down
```

Or, if using the Kerberos configuration:
```
docker-compose -f docker-compose-kerberos.yml down
```

This will shut down and remove the containers safely.
