from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark import sql

# Create the spark session
spark = SparkSession \
    .builder \
    .appName("Vertica Connector Pyspark Example App") \
    .getOrCreate()
spark_context = spark.sparkContext
sql_context = sql.SQLContext(spark_context)

# Set connector options based on our Docker setup
host="vertica"
user="dbadmin"
password=""
db="docker"
staging_fs_url="hdfs://hdfs:8020/data/dirtest"
table="pysparktest"

# Define data to write to Vertica
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
# Create an RDD from the data
rdd = spark_context.parallelize(data)
# Convert the RDD to a DataFrame
df = rdd.toDF(columns)
# Write the DataFrame to the Vertica table pysparktest
df.write.mode('overwrite').save(format="com.vertica.spark.datasource.VerticaSource",
 host="vertica",
 user="dbadmin",
 password="",
 db="docker",
 staging_fs_url="webhdfs://hdfs:50070/data/dirtest",
 table="pysparktest")

# Read the data back into a Spark DataFrame
readDf = spark.read.load(format="com.vertica.spark.datasource.VerticaSource",
 host="vertica",
 user="dbadmin",
 password="",
 db="docker",
 table="pysparktest",
 staging_fs_url="hdfs://hdfs:8020/data/dirtest")
# Print the DataFrame contents
print(readDf.rdd.collect())
