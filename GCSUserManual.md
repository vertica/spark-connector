# Google Cloud Storage Integration Guide

Since Vertica can be deployed on Google Cloud Platform, it is possible for the Spark Connector to make use of Google Cloud Storage as the intermediary storage.

 * **Running on DataProc clusters:** If your Spark cluster deployed on GCP, you will need to obtain a GCS service account HMAC key to configure connector options `gcs_hmac_key_id` and `gcs_hmac_key_secret`. 
The instruction on obtaining the HMAC key can be found [here](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).
 * **Running out-side of DataProc clusters:** If you only wish to use GCS as the staging area, then in addition to configuring the HMAC key above, you will obtain a GCS service account key in the form of a json service keyfile. Instruction on obtaining one can be found [here](https://cloud.google.com/storage/docs/authentication#generating-a-private-key).
Then, specify the connector option `gcs_keyfile` with the path to your keyfile JSON. Alternatively, if you specify the path in the environment variable `GOOGLE_APPLICATION_CREDENTIALS` then you do not have to specify the connector option. 
Finally, ensure that you include the [Google Hadoop Connector](https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector) dependency into your project. Make sure your select the appropriate connector distribution for your Hadoop version.

With the credential specified, you can now configure the connector option `staging_fs_url` to use GCS paths `gs://<bucket-id>/path/to/data`.

###Additional Resources
 * [Google Hadoop Connector GitHub](https://github.com/GoogleCloudDataproc/hadoop-connectors)
 * [Using Google Hadoop Connector](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)