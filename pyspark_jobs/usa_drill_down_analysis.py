# Task 7: USA Drilldown Analysis

# Aggregate county data to state level.
# Identify top 10 affected states.
# Detect data skew across states.
# Explain skew impact in distributed systems.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Recovery Efficiency Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# HDFS paths
staging_path = "hdfs://localhost:9000/data/covid/staging/"
analytics_path = "hdfs://localhost:9000/data/covid/analytics/"

# Read Parquet datasets from HDFS
usa_df = spark.read.parquet(staging_path + "usa_county_wise.parquet")

usa_df.show()

# 1.Aggregate county data to state level.
aggregate_df = usa_df.groupBy("Province_State","Country_Region")\
    .agg(avg("Lat").alias("Avg_Lat"),\
         avg("Long_").alias("Avg_Long_"),\
         count("Admin2").alias("No_of_Counties"),\
         sum("Confirmed").alias("Total_affected"),\
         sum("Deaths").alias("Total_deaths")
        )
aggregate_df.write.mode("overwrite") \
    .parquet(analytics_path + "aggregate_county_data.parquet")

aggregate_df.show()

# 2.Identify top 10 affected states.
affected_df = aggregate_df.orderBy(col("Total_affected").desc())\
             .select("Province_State", "Country_Region","Total_affected").limit(10)
affected_df.write.mode("overwrite") \
    .parquet(analytics_path + "top_10_affected_states.parquet")

affected_df.show()

# 3.Detect data skew across states.
state_distribution = usa_df.groupBy("Province_State") \
    .agg(count("*").alias("record_count")) \
    .orderBy("record_count", ascending=False)

state_distribution = state_distribution.describe()
state_distribution.write.mode("overwrite") \
    .parquet(analytics_path + "data_scew_across_states.parquet")

state_distribution.show()

# 4.Explain skew impact in distributed systems.
# Data skew occurs when some keys contain significantly more records than others,
# causing uneven partition sizes during shuffle operations in Spark.
# This leads to straggler tasks, memory pressure, spill-to-disk, and increased job execution time.







