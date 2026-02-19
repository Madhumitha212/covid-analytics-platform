from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("COVID Infection Rate Analysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# HDFS paths
staging_path = "hdfs://localhost:9000/data/covid/staging/"
analytics_path = "hdfs://localhost:9000/data/covid/analytics/"

world_df = spark.read.parquet(staging_path + "worldometer_data.parquet")

#Confirmed cases per 1000 population.
#Active cases per 1000 population.

confirmed_cases = world_df.withColumn(
    "Confirmed Cases per 1000", 
    round((col("TotalCases")/col("Population")*1000),2)
    ).withColumn(
    "Active per 1000",
    round((col("ActiveCases")/col("Population")*1000),2)
    )

confirmed_cases.write.mode("overwrite")\
    .parquet(analytics_path + "Confirmed_active_per_1000.parquet")

#Top 10 countries by infection rate

top_10 = confirmed_cases.orderBy(
    col("Confirmed Cases per 1000").desc()
).limit(10)

top_10.write.mode("overwrite")\
    .parquet(analytics_path + "top_10_countries_infection_rate")

top_10.show()



