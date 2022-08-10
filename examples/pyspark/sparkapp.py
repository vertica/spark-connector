from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark import sql

# Create the spark session
spark = SparkSession \
    .builder \
    .appName("Vertica Connector Pyspark Example") \
    .getOrCreate()
spark_context = spark.sparkContext
sql_context = sql.SQLContext(spark_context)

# The name of our connector for Spark to look up
format = "com.vertica.spark.datasource.VerticaSource"

# Set connector options based on our Docker setup
host="vertica"
user="dbadmin"
password=""
db="docker"
staging_fs_url="webhdfs://hdfs:50070/data/"
table="pysparktest"

# Define data to write to Vertica
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
# Create an RDD from the data
rdd = spark_context.parallelize(data)
# Convert the RDD to a DataFrame
df = rdd.toDF(columns)
# Write the DataFrame to the Vertica table pysparktest
df.write.mode('overwrite').save(
 # Spark format
 format=format,
 # Connector specific options
 host=host,
 user=user,
 password=password,
 db=db,
 staging_fs_url=staging_fs_url,
 table=table)

# Read the data back into a Spark DataFrame
readDf = spark.read.load(
 # Spark format
 format=format,
 # Connector specific options
 host=host,
 user=user,
 password=password,
 db=db,
 table=table,
 staging_fs_url=staging_fs_url)

# Print the DataFrame contents
readDf.show()
