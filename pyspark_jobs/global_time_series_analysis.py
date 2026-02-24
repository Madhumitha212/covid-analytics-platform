# Task 6: Global Time-Series Analysis

# Global daily average new cases.
# Detect spike days using Z-score.
# Identify peak death date globally.
# Month-over-Month death growth rate.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Global Time-Series Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# HDFS paths
staging_path = "hdfs://localhost:9000/data/covid/staging/"
analytics_path = "hdfs://localhost:9000/data/covid/analytics/"

#Read parquet dataset from hdfs
# 1.Global daily average new cases.
day_df = spark.read.parquet(staging_path+"day_wise.parquet")

windowSpec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
daily_avg = day_df.withColumn("Avg_new_cases", 
                              round(avg("New cases").over(windowSpec), 2
                             ))
daily_avg.write.mode("overwrite") \
    .parquet(analytics_path + "global_daily_average_newcase.parquet")

daily_avg.select("Date", "New cases", "Avg_new_cases").show()

# 2.Detect spike days using Z-score.
global_daily = day_df.groupBy("Date") \
    .agg(sum("New cases").alias("Global_NewCases"))

stats = global_daily.agg(
    avg("Global_NewCases").alias("mean_cases"),
    stddev("Global_NewCases").alias("std_cases")
)

mean = stats.collect()[0]["mean_cases"]
std_dev = stats.collect()[0]["std_cases"]

spike_days = global_daily.withColumn("Z-score", round(
    (col("Global_NewCases") - mean)/std_dev, 2)
                       ).filter(col("Z-score")>2)
spike_days.write.mode("overwrite") \
    .parquet(analytics_path + "spike_days_using_zscore.parquet")

spike_days.show()


# 3.Identify peak death date globally.
global_death_df = day_df.groupBy("Date").agg(sum("New deaths").alias("Global_new_deaths"))

peak_death_date = global_death_df.orderBy(col("Global_new_deaths").desc())
peak_death_date.write.mode("overwrite") \
    .parquet(analytics_path + "global_peak_death_date.parquet")

peak_death_date.show(1)

# 4.Month-over-Month death growth rate.
day_df = day_df.withColumn("Month", month("Date"))\
                .withColumn("Year", year("Date"))

monthly_deaths = day_df.groupBy("Month","year")\
    .agg(sum("New deaths").alias("Monthly_deaths")).orderBy("Month","Year")

windowSpec = Window.orderBy("Month", "year")

death_growth = monthly_deaths.withColumn("Previous_month_death" , lag("Monthly_deaths").over(windowSpec))

death_growth = death_growth.withColumn("death_growth", round(
    ((col("Monthly_deaths") - col("Previous_month_death"))/col("Previous_month_death"))*100 , 2))

death_growth.write.mode("overwrite") \
    .parquet(analytics_path + "month_death_growth_rate.parquet")
death_growth.show()
