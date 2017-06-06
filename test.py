# # Test access to HDFS and Spark cluster

# Copy local file to HDFS.
!hdfs dfs -put test.csv /tmp

# Import SparkSession.
from pyspark.sql import SparkSession

# Create SparkSession.
spark = SparkSession.builder.appName('test').getOrCreate()

# Load file into DataFrame.
df = spark.read.csv('/tmp/test.csv', header=True, inferSchema=True)

# Print schema.
df.printSchema()

# View DataFrame.
df.show()

# Stop SparkSession.
spark.stop()

# Remove file from HDFS.
!hdfs dfs -rm /tmp/test.csv

