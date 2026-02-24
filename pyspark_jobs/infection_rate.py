# Task 4: Infection Rate Analysis

# Confirmed cases per 1000 population.
# Active cases per 1000 population.
# Top 10 countries by infection rate.
# WHO region infection ranking.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("COVID Infection Rate Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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
confirmed_cases.show()

#Top 10 countries by infection rate

top_10 = confirmed_cases.orderBy(
    col("Confirmed Cases per 1000").desc()
).limit(10)

top_10.write.mode("overwrite")\
    .parquet(analytics_path + "top_10_countries_infection_rate.parquet")

top_10.show()


#WHO region infection ranking
covid_clean_df = spark.read.parquet(staging_path+"covid_19_clean_complete.parquet")

who_mapping = covid_clean_df.select(
    "Country/Region",
    "WHO Region"
)

joined_df = world_df.join(
    who_mapping,
    on="Country/Region",
    how="inner"
)

who_rank= joined_df.groupBy("WHO Region").agg(
    sum("TotalCases").alias("Total_Cases"),
    sum("Population").alias("Total_Population")
)

who_rank = who_rank.withColumn(
    "Confirmed Cases",
    round((col("Total_Cases")/col("Total_Population")*1000),2)
)

who_rank = who_rank.orderBy("Confirmed Cases").desc()
who_rank.write.mode("overwrite")\
    .parquet(analytics_path + "WHO_region_ranking.parquet")

who_rank.show()






