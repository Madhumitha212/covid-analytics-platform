# Task 3: Death Percentage Analysis

# 1.Compute daily death percentage per country:
#   Deaths / Confirmed * 100
# 2.Compute global daily death percentage.
# 3.Compute continent-wise death percentage (join with worldometer_data).
# 4.Identify:
#  - Country with highest death percentage
#  - Top 10 countries by deaths per capita
# All results must be written to HDFS under /data/covid/analytics.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum , round , desc,  max, when
from pyspark.sql.window import Window

# Spark session

spark = SparkSession.builder \
    .appName("COVID Death Percentage Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# HDFS paths
staging_path = "hdfs://localhost:9000/data/covid/staging/"
analytics_path = "hdfs://localhost:9000/data/covid/analytics/"

# Read Parquet datasets from HDFS
full_df = spark.read.parquet(staging_path + "full_grouped.parquet")
world_df = spark.read.parquet(staging_path + "worldometer_data.parquet")

# 1. Daily death percentage per country

country_daily_death = full_df.withColumn(
    "death_percentage",
    when(col("Confirmed") == 0, 0)
    .otherwise(round((col("Deaths") / col("Confirmed")) * 100, 2))
)

# Save to HDFS
country_daily_death.write.mode("overwrite") \
    .parquet(analytics_path + "country_daily_death_percentage.parquet")

print("Country Daily Death % Preview:")
country_daily_death.select("Date", "Country/Region", "death_percentage").show()

# 2. Global daily death percentage

global_daily = full_df.groupBy("Date").agg(
    sum("Deaths").alias("total_deaths"),
    sum("Confirmed").alias("total_confirmed")
).withColumn(
    "global_death_percentage",
    round((col("total_deaths") / col("total_confirmed")) * 100, 2)
)

global_daily.write.mode("overwrite") \
    .parquet(analytics_path + "global_daily_death_percentage.parquet")

print("Global Daily Death % Preview:")
global_daily.select("Date", "global_death_percentage").show(5)

# 3. Continent-wise daily death percentage

continent_df = full_df.join(
    world_df.select("Country/Region", "Continent", "Population"),
    on="Country/Region",
    how="left"
)

continent_death = continent_df.groupBy("Continent", "Date").agg(
    sum("Deaths").alias("total_deaths"),
    sum("Confirmed").alias("total_confirmed")
).withColumn(
    "continent_death_percentage",
    round((col("total_deaths") / col("total_confirmed")) * 100, 2)
)

continent_death.write.mode("overwrite") \
    .parquet(analytics_path + "continent_daily_death_percentage.parquet")
print("Continent-wise Death % Preview:")
continent_death.show()

# 4. Country with highest death percentage 
latest_death = country_daily_death.orderBy(desc("death_percentage")).limit(1)
latest_death.write.mode("overwrite") \
    .parquet(analytics_path + "highest_death_country.parquet")

print("Country with highest death percentage")
latest_death.show(1)


# 5. Top 10 countries by deaths per capita
deaths_per_capita = world_df.withColumn(
    "Deaths_per_Capita",
    col("TotalDeaths") / col("Population")
)

top10 = deaths_per_capita.orderBy(
    col("Deaths_per_Capita").desc()
).select(
    "Country/Region",
    "Deaths_per_Capita"
).limit(10)

top10.write.mode("overwrite") \
    .parquet(analytics_path + "top10_death_per_capita.parquet")

print("Top 10 countries death per capita")
top10.show()

print("Task 3 completed Death Percentage Analysis saved to HDFS.")
