# Google Cloud Storage Integration Guide

Since Vertica can be deployed on Google Cloud Platform, it is possible for the Spark Connector to make use of Google Cloud Storage as the intermediary storage. 
You will need to prepare the correct credentials in order for the connector to connect Spark and Vertica to your GCS project.

 * **Running on DataProc clusters:** If your Spark application is submitted to GCP, you will need to obtain a GCS HMAC key and configure connector options `gcs_hmac_key_id` and `gcs_hmac_key_secret`. 
The instruction on obtaining the HMAC key can be found [here](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).
 * **Running out-side of DataProc clusters:** In addition to configuring the HMAC key above, you will obtain a GCS service account key in the form of a json service keyfile. Instruction on obtaining one can be found [here](https://cloud.google.com/storage/docs/authentication#generating-a-private-key).
Then, specify the connector option `gcs_keyfile` with the path to your keyfile JSON. Alternatively, if you specify the path in the environment variable `GOOGLE_APPLICATION_CREDENTIALS` then you do not have to specify the connector option. 
Finally, ensure that you include the [Google Hadoop Connector](https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector) dependency into your project.