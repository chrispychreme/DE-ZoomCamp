import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("InferSchema", "true") \
    .parquet('fhv_tripdata_2019-10.parquet')

df = df.repartition(6)

df.write.parquet('fhv/2019/10/', mode='overwrite')

df = spark.read.parquet('fhv/2019/10/')

# print(df.printSchema())
q3df = df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .select('pickup_date', 'pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID') \
    .filter(F.to_date(df.pickup_datetime) == '2019-10-05')

print(q3df.count())