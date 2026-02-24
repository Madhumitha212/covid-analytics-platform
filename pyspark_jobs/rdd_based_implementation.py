# Task 8: RDD-Based Implementation

# Calculate total confirmed per country.
# Calculate total deaths per country.
# Compute death percentage using reduceByKey.
# Compare RDD performance vs DataFrame.
# Explain:
#  1.Why reduceByKey is preferred over groupByKey
#  2.When RDD should be avoided

from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("RDD Implementation") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.textFile("hdfs:///data/covid/raw/full_grouped.csv")
header = rdd.first()
data = rdd.filter(lambda row: row != header)

#split columns
split_rdd = data.map(lambda x: x.split(","))

result = split_rdd.take(5)
print(result)

# 1.Calculate total confirmed per country.
country_confirmed = split_rdd.map(
    lambda x: (x[1], int(x[2]))
)

total_confirmed = country_confirmed.reduceByKey(
    lambda a, b: a + b
)

result = total_confirmed.take(5)
print(f"Total confirmed: {result}")

# 2.Calculate total deaths per country.

country_deaths = split_rdd.map(
    lambda x: (x[1], int(x[3]))
)

total_deaths = country_deaths.reduceByKey(
    lambda a, b: a + b
)

result = total_deaths.take(5)
print(f"Total deaths: {result}")

# 3.Compute death percentage using reduceByKey.
aggregated = total_confirmed.join(total_deaths)

death_percentage = aggregated.mapValues(
    lambda x: (x[1] / x[0]) * 100 if x[0] != 0 else 0
)
result = death_percentage.take(5)
print(f"Death percentage: {result}")

df_result = death_percentage.toDF(["Country", "Death_Percentage"])

df_result.write.mode("overwrite").parquet(
    "hdfs:///data/covid/analytics/rdd_death_percentage"
)

# 4.Compare RDD performance vs DataFrame.
start = time.time()
rdd_full_grouped = spark.sparkContext.textFile("hdfs:///data/covid/raw/full_grouped.csv")
header = rdd_full_grouped.first()
rdd_full_grouped = rdd_full_grouped.filter(lambda x: x != header)
rdd_country_confirmed = rdd_full_grouped.map(lambda x: x.split(",")) \
    .map(lambda columns: (columns[1], int(columns[2]))) \
    .reduceByKey(lambda confirmed1, confirmed2: confirmed1 + confirmed2)
rdd_country_confirmed.count()
rdd_time = time.time() - start
print(f"RDD Execution Time: {rdd_time:.2f} seconds")

start = time.time()
df_full_grouped = spark.read.csv("hdfs:///data/covid/raw/full_grouped.csv",header=True,inferSchema=True)
df_country_confirmed = df_full_grouped.groupBy("Country/Region").sum("Confirmed")
df_country_confirmed.count()
df_time = time.time() - start
print(f"DataFrame Execution Time: {df_time:.2f} seconds")

if rdd_time > df_time:
    print("DataFrame is faster")
else:
    print("RDD is faster")

print("=========Task 8 completed=======")

# 5.Why reduceByKey is preferred over groupByKey
# ReduceByKey performs aggregation on each partition before shuffling 
# less data transfer.

# 5.When RDD should be avoided
# GroupByKey shuffles all values of a key across the network 
# Heavy memory and network usage.

# 5.When RDD should be avoided
# RDD should be avoided when working with structured data or large datasets
# Large datasets lacks optimization and it is slower.
