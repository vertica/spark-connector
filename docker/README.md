# Docker Environment

We provide a docker environment for testing and developing that contains the following containers:
- `docker_client_1`: The client environment. This is where spark application are submitted/executed from
- `docker_hdfs_1`: The HDFS node acting as the staging file-store
- `docker_vertica_1`: Our Vertica database
- `docker_minio_1`: A minio file-store container acting as an S3 file-store 
- `spark`: A Spark driver for our standalone cluster
- `spark-worker-*`: Spark workers for our standalone cluster, created in swarm mode

### Starting the Environment

To start, make sure you have Docker and sbt installed, and that Docker client is running.
Then, clone the repository if you haven't already:

```
git clone https://github.com/vertica/spark-connector.git
```

use `./sandbox-clientenv.bat` for Windows or `./sandbox-clientenv.sh` for Linux based system.

If on Windows, make sure that all scripts in the repository are correctly encoded:

- `.bat` scripts as `Windows (CR LF)`
- `.sh` scripts as `Unix (LF)`.

Once started, you can visit the Spark standalone cluster [Master WebUI](localhost:8080) and the first Spark context UI at 
localhost:4040 (won't be available until a context is created).

You can exit the container's shell with `exit`, and get into other container's shell using `docker exec -it <container-name> bash`

### Configurations

The `sandbox-clientenv` scripts has the following arguments:
- `-v`: Setting the version of our Vertica database. By default, we use the [latest](https://hub.docker.com/r/vertica/vertica-k8s) Vertica docker image. 
To use an older version of Vertica, you can specify a specific tag by appending the option `-v [TAG]`. For example, to use Vertica 10.1.1-0 use `./sandbox-clientenv.sh -v 10.1.1-0`
Alternatively, you can set the environment variable `VERTICA_VERSION`.
- `-s`: Setting the version of our Spark install. By default, we use the [latest](https://hub.docker.com/r/bitnami/spark) bitnami Spark image. To use an older
Spark install, specify a bitnami image tag. For example, to set up for Spark 3.1.3 use `./sandbox-clientenv.sh -s 3.1.3`.
Alternatively, you can set the environment variable `SPARK_INSTALL`.
- `-w`: Set the number of Worker nodes for the standalone cluster. Default to 1 worker.
- `-k`: When specified, start the docker environment in Kerberos mode. **This option is incompatible with the above options**.

For other configurations parameters, since we are using bitnami as the base image of our cluster and `docker_client_1`, 
refers to their [available options](https://hub.docker.com/r/bitnami/spark) and set the appropriate environment variables in `docker-compose.yml`.

### Tear Down Containers

Once you're finished running the example, exit out of the interactive terminal by running `exit`.

Make sure you are still in the docker directory, then run:
```
docker-compose down
```
This will shut down and remove the containers safely.
