# Task 2: Data Ingestion & Optimization
#  1.Read all raw CSV files from HDFS.
#  2.Apply proper schema instead of inferSchema.
#  3.Handle null values.
#  4.Convert raw CSV files into Parquet format.
#  5.Store them in /data/covid/staging.
#  6.Compare CSV vs Parquet:
#  -File size
#  -Read performance
#  -Execution plan

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("COVID Analytics") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

country_latest_schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("New cases", LongType(), True),
    StructField("New deaths", LongType(), True),
    StructField("New recovered", LongType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True)
])

country_df = spark.read.csv(
    "hdfs:///data/covid/raw/country_wise_latest.csv",
    header=True,
    schema=country_latest_schema
)

numeric_cols = [
    "Confirmed","Deaths","Recovered","Active",
    "New cases","New deaths","New recovered","Deaths / 100 Cases", "Recovered / 100 Cases"
]

country_df = country_df.fillna(0, subset=numeric_cols)
country_df = country_df.fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/country_wise_latest.parquet"
country_df.write.mode("overwrite").parquet(hdfs_path)

country_df.show(5)

clean_complete_schema = StructType([
    StructField("Province/State", StringType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Date", DateType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("WHO Region", StringType(), True)
    
])

covid_clean_df = spark.read.csv(
    "hdfs:///data/covid/raw/covid_19_clean_complete.csv",
    header=True,
    schema=clean_complete_schema
)

numeric_cols = [
     "Lat",	"Long", "Date",	"Confirmed", "Deaths", "Recovered",	"Active"
]
covid_clean_df = covid_clean_df.fillna(0, subset=numeric_cols)
covid_clean_df = covid_clean_df.fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/covid_19_clean_complete.parquet"
covid_clean_df.write.mode("overwrite").parquet(hdfs_path)

covid_clean_df.show(5)

day_schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New cases", IntegerType(), True),
    StructField("New deaths", IntegerType(), True),
    StructField("New recovered", IntegerType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True)
])

day_df = spark.read.csv(
    "hdfs:///data/covid/raw/day_wise.csv",
    header=True,
    schema=day_schema
)

numeric_cols = [
     "Confirmed", "Deaths", "Recovered", "Active", "New cases", "New recovered",	"Deaths / 100 Cases", "Recovered / 100 Cases"
]
day_df  = day_df .fillna(0, subset=numeric_cols)
day_df  = day_df .fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/day_wise.parquet"
day_df.write.mode("overwrite").parquet(hdfs_path)

day_df.show(5)

full_grouped_schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", IntegerType(), True),
    StructField("Deaths", IntegerType(), True),
    StructField("Recovered", IntegerType(), True),
    StructField("Active", IntegerType(), True),
    StructField("New cases", IntegerType(), True),
    StructField("New deaths", IntegerType(), True),
    StructField("New recovered", IntegerType(), True),
    StructField("WHO Region", StringType(), True)
])

full_df = spark.read.csv(
    "hdfs:///data/covid/raw/full_grouped.csv", 
    header=True,
    schema=full_grouped_schema
)

numeric_cols = [
     "Confirmed", "Deaths", "Recovered", "Active", "New cases", "New deaths", "New recovered"
]
full_df  = full_df .fillna(0, subset=numeric_cols)
full_df  = full_df .fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/full_grouped.parquet"
full_df.write.mode("overwrite").parquet(hdfs_path)

full_df.show(5)

usa_country_schema = StructType([
    StructField("UID", LongType(), True),
    StructField("iso2", StringType(), True),
    StructField("iso3", StringType(), True),
    StructField("code3", IntegerType(), True),
    StructField("FIPS", StringType(), True),
    StructField("Admin2", StringType(), True),
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long_", DoubleType(), True)
])

usa_df = spark.read.csv(
    "hdfs:///data/covid/raw/usa_country_wise.csv", 
    header=True,
    schema=usa_country_schema
)

numeric_cols = [
     "UID", "code3", "Lat", "Long_"
]
usa_df  = usa_df .fillna(0, subset=numeric_cols)
usa_df  = usa_df .fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/usa_county_wise.parquet"
usa_df.write.mode("overwrite").parquet(hdfs_path)

usa_df.show(5)

worldometer_schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Continent", StringType(), True),
    StructField("Population", LongType(), True),
    StructField("TotalCases", LongType(), True),
    StructField("NewCases", IntegerType(), True),
    StructField("TotalDeaths", LongType(), True),
    StructField("NewDeaths", IntegerType(), True),
    StructField("TotalRecovered", LongType(), True),
    StructField("NewRecovered", IntegerType(), True),
    StructField("ActiveCases", LongType(), True)
])

worldometer_df = spark.read.csv(
    "hdfs:///data/covid/raw/worldometer_data.csv", 
    header=True,
    schema=worldometer_schema
)

numeric_cols = [
     "Population", "TotalCases", "NewCases","TotalDeaths", "NewDeaths", "TotalRecovered", "NewRecovered", "ActiveCases"
]
worldometer_df  = worldometer_df .fillna(0, subset=numeric_cols)
worldometer_df  = worldometer_df .fillna("Unknown")

hdfs_path = "hdfs:///data/covid/staging/worldometer_data.parquet"
worldometer_df.write.mode("overwrite").parquet(hdfs_path)

worldometer_df.show(5)



