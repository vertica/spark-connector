## Google Cloud Storage Example

To start, follow the instruction [here](https://github.com/vertica/spark-connector/blob/main/GCSUserManual.md)
to obtain the necessary credentials, then edit the appropriate configurations in `./src/main/resources/application.conf`.

The example will use GCS as the intermediary area to write a simple table to Vertica before read it back.

To run the example locally, start the docker environment then navigate to this example's folder. Then execute
```
sbt run
```

Else, compile the example into a jar and submit to your cluster

