# Docker Environment

We provide a docker environment for testing and developing that contains the following containers:
- `docker-client-1`: The client environment. This is where spark application are submitted/executed from
- `docker-hdfs-1`: The HDFS node acting as the staging file-store
- `docker-jupyter-1`: A Jupyter Notebook that can be used to test the Spark Connector
- `docker-minio-1`: A minio file-store container acting as an S3 file-store
- `docker-spark-1`: A Spark driver for our standalone cluster
- `docker-spark-worker-*`: Spark workers for our standalone cluster, created in swarm mode
- `docker-vertica-1`: Our Vertica database

When using the Kerberos configuration the container names are simplified (`client`, `hdfs`, and `vertica`, plus one for the Kerberos key server, `kdc`).  The Spark and MinIO containers are not currently deployed for Kerberos.

Note that the container names listed here may not be exactly the same in all environments.  For example, under Docker Compose V1 the container might be named `docker_client_1`, and under V2 `docker-client-1`.

### Installing Docker

The most convenient method of using Docker (and Docker Compose) is to install [Docker Desktop](https://www.docker.com/products/docker-desktop/).  It is recommended to install a recent version, with Docker Engine version 20.10.0+ and Docker Compose version 1.29.0+.

In most cases, the `docker-compose` commands can be substituted with `docker compose` commands.  `docker-compose` is the standalone CLI, while Compose is also part of Docker now (e.g. `docker compose`).

For some Linux distros, if you get an error about `docker-credential-secretservice` not installed, install that package with the package manager on your system.  For example, on Ubuntu:
```sh
sudo apt install golang-docker-credential-helpers
```

### Starting the Environment

To start, make sure you have Docker and sbt installed, and that Docker client is running.

Then, clone the repository if you haven't already:
```sh
git clone https://github.com/vertica/spark-connector.git
```

Navigate to the `docker` folder:
```sh
cd spark-connector/docker
```

Then use Docker Compose to start the containers:
```sh
docker-compose up -d
```

Or, for the Kerberos configuration:
```sh
docker-compose -f docker-compose-kerberos.yml up -d
```

Once started, you can visit the Spark standalone cluster [Master WebUI](localhost:8080) and the first Spark context UI at 
localhost:4040 (won't be available until a context is created).

You can access a container shell as follows:
```sh
docker exec -it <container-name> bash

# Client container example:
docker exec -it docker-client-1 bash
# or
docker exec -it krb-client bash
```

You can exit the container's shell with `exit`.

Note that it can take a few seconds for some of the services, notably Vertica and HDFS, to initialize.  Before running any tests, ensure that the services are up by looking for the "\<service\> container is now running" message at the end of the logs:
```sh
docker logs docker-vertica-1
docker logs docker-hdfs-1
```

### Spark and Vertica Versions

To use a specific version set the following environment variables in your environment, such as in your Docker `.env` file:
- `VERTICA_VERSION`: By default, we use the latest [Vertica](https://hub.docker.com/r/vertica/vertica-k8s) image. For example, to use Vertica 10.1.1-0 set `VERTICA_VERSION=10.1.1-0`.
- `SPARK_INSTALL`: By default, we use the latest [Bitnami Spark](https://hub.docker.com/r/bitnami/spark) image. For example, to use Spark 3.1.3 set `SPARK_INSTALL=3.1.3`.

By default we start a single Spark worker.  To start multiple Spark workers, such as 3 workers:
```sh
docker-compose up -d --scale spark-worker=3
```

For other Spark configurations parameters refer to available options for the [Bitnami Spark](https://hub.docker.com/r/bitnami/spark) image and set the appropriate environment variables in `docker-compose.yml` or your `.env` file.

### Jupyter Notebook

By default the Jupyter Notebook container is not started as the image is quite large and it is only needed when testing the Spark Connector in Jupyter Notebook.

To start (or stop) the Jupyter Notebook, specify the "jupyter" profile when managing the containers:
```sh
docker-compose --profile jupyter up -d

docker-compose --profile jupyter down
```

### Tear Down the Environment

Once you're finished running the example, exit out of the interactive terminal by running `exit`.

Make sure you are still in the docker directory, then run:
```sh
docker-compose down
```

Or, if using the Kerberos configuration:
```sh
docker-compose -f docker-compose-kerberos.yml down
```

This will shut down and remove the containers safely.
