# Copy local file to HDFS.
!hdfs dfs -put test.csv /tmp

# Import SparkSession.
from pyspark.sql import SparkSession

# Create SparkSession.
spark = SparkSession.builder.appName('test').getOrCreate()

# Load file into DataFrame.
df = spark.read.csv('/tmp/people.txt', header=True, inferSchema=True)

# View DataFrame.
df.show()
