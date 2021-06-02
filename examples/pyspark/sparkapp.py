from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark import sql

spark = SparkSession \
    .builder \
    .appName("Vertica Connector Pyspark Example App") \
    .getOrCreate()
sc = spark.sparkContext
sqlContext = sql.SQLContext(sc)

host="vertica"
user="dbadmin"
password=""
db="docker"
staging_fs_url="hdfs://hdfs:8020/data/dirtest"
table="pysparktest"

# Write data to table
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = sc.parallelize(data)
df = rdd.toDF(columns)
df.write.mode('overwrite').save(format="com.vertica.spark.datasource.VerticaSource",
 host="vertica",
 user="dbadmin",
 password="",
 db="docker",
 staging_fs_url="hdfs://hdfs:8020/data/dirtest",
 table="pysparktest")

# Read back into Spark
readDf = spark.read.load(format="com.vertica.spark.datasource.VerticaSource",
 host="vertica",
 user="dbadmin",
 password="",
 db="docker",
 table="pysparktest",
 staging_fs_url="hdfs://hdfs:8020/data/dirtest")
print(readDf.rdd.collect())
