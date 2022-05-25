## Google Cloud Storage Example

To use GCS as the intermediary area, we need to obtain the necessary credentials. The instructions can be found [here](../../GCSUserManual.md)
, then edit the appropriate configurations in `./src/main/resources/application.conf`.

To run the example locally, [start the docker environment](../README.md#Prepare test environment) then navigate to this example's folder. Then execute
```
sbt run
```

Else, compile the example into a fat jar with `sbt assembly` and submit to your cluster.

