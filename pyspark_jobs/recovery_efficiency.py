# Task 5: Recovery Efficiency

# Recovered percentage per country.
# 7-day rolling recovery average (Window function).
# Country with fastest recovery growth.
# Peak recovery day per country.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("COVID Infection Rate Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# HDFS paths
staging_path = "hdfs://localhost:9000/data/covid/staging/"
analytics_path = "hdfs://localhost:9000/data/covid/analytics/"

# Read Parquet datasets from HDFS
world_df = spark.read.parquet(staging_path + "worldometer_data.parquet")

# 1.Recovered percentage per country.
recovery_df = world_df.withColumn(
    "Recovery_Percentage",
    round((col("TotalRecovered") / col("TotalCases")) * 100, 2)
)

final_df = recovery_df.select(
    "Country/Region",
    "Recovery_Percentage"
)

final_df.write.mode("overwrite") \
    .parquet(analytics_path + "recovered_percentage_per_country.parquet")

final_df.show()

# 2.7-day rolling recovery average.
day_df = spark.read.parquet(staging_path + "day_wise.parquet")

windowSpec = Window.orderBy("Date").rowsBetween(-6,0)

roll_df = day_df.withColumn(
    "7_Day_Rolling_Average",round(avg(col("Recovered")).over(windowSpec),2)
).select("Date", "Recovered","7_Day_Rolling_Average")

roll_df.write.mode("overwrite") \
    .parquet(analytics_path + "7_day_rolling_average.parquet")

roll_df.show()

#Country with fastest recovery growth.
full_df = spark.read.parquet(staging_path+"full_grouped.parquet")

full_df = full_df.withColumn("Recovery_percentage",
                             when(col("Confirmed")>0,
                            round(col("Recovered")/col("Confirmed")*100,2))
                            .otherwise(0))

windowSpec = Window.partitionBy("Country/Region").orderBy("Date")

recovery_df = full_df.withColumn("Previous_recovery",
                                lag("Recovery_percentage",1).over(windowSpec))

recovery_growth = recovery_df.withColumn("Recovery_growth",
                                      round(
                                      col("Recovery_percentage")-col("Previous_recovery"),2
                                      ))

recovery_growth = recovery_growth.filter(col("Confirmed") > 1000)

recovery_growth = recovery_growth.orderBy(col("Recovery_growth").desc()).select("Date","Country/Region",
                                            "Recovery_percentage","Previous_recovery"
                                            ,"Recovery_growth")

recovery_growth.write.mode("overwrite") \
    .parquet(analytics_path + "country_fastest_recovery_growth.parquet")

recovery_growth.show()

#Peak recovery day per country.
peak_window = Window.partitionBy("Country/Region") \
                    .orderBy(desc("Recovery_growth"))

peak_df = recovery_growth.withColumn(
    "rank",
    row_number().over(peak_window)
)

peak_recovery_day = peak_df.filter(col("rank") == 1) \
    .select("Country/Region", "Date", "Recovery_growth")

peak_recovery_day.write.mode("overwrite") \
    .parquet(analytics_path + "peak_recovery_day.parquet")

peak_recovery_day.show()