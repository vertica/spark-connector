# Spark Connector

This component acts as a bridge between Spark and Vertica, allowing the user to either retrieve data from Vertica for processing in Spark, or store processed data from Spark into Vertica.

Why is this connector desired instead of using a more generic JDBC connector? A few reasons:
* Vertica-specific syntax and features. This connector can support things like Vertica projections
* Authentication to the Vertica server 
* Segmentation. We can use the way that Vertica segments data to inform our operations. 
* Ability to use other technology such as HDFS/S3 as an intermediary for data transfer. This is necessary for maximizing performance of parallel loads.

