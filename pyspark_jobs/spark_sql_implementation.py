# Task 9: Spark SQL Implementation

# Create temporary views.
# Write SQL queries for:
#  1.Top 10 infection countries
#  2.Death percentage ranking
#  3.Rolling 7-day average
# Compare physical plans with DataFrame API.

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark sql implememtation") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs:///data/covid/staging/full_grouped.parquet")

# create temporary view
df.createOrReplaceTempView("covid_data")

# 1.Top 10 infection countries
top10 = spark.sql("""
SELECT `Country/Region`,
       SUM(Confirmed) AS total_confirmed
FROM covid_data
GROUP BY `Country/Region`
ORDER BY total_confirmed DESC
LIMIT 10
""")
top10.show()

# 2.Death percentage ranking
death_rank = spark.sql("""
SELECT `Country/Region`,
       (SUM(Deaths)/SUM(Confirmed))*100 AS death_percentage
FROM covid_data
GROUP BY `Country/Region`
ORDER BY death_percentage DESC
""")
death_rank.show()

# 3.Rolling 7-day average
rolling = spark.sql("""
SELECT Date,
       round(AVG(`New cases`) OVER (
           PARTITION BY `Country/Region`
           ORDER BY Date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ), 2) AS rolling_avg
FROM covid_data
WHERE Date > '2020-04-01'
ORDER BY `Country/Region`, Date
""")
rolling.show()

# 4.Compare physical plans with DataFrame API.
top10.explain()

df.groupBy("Country/Region") \
  .sum("Confirmed") \
  .orderBy("sum(Confirmed)", ascending=False) \
  .limit(10) \
  .explain()

