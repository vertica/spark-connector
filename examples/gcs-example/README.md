## Google Cloud Storage Example

To use GCS as the intermediary area, we need to obtain the necessary credentials. The instructions can be found [here](https://github.com/vertica/spark-connector/blob/main/GCSUserManual.md)
, then edit the appropriate configurations in `./src/main/resources/application.conf`.

To run the example locally, start the docker environment then navigate to this example's folder. Then execute
```
sbt run
```

Else, compile the example into a jar and submit to your cluster.

