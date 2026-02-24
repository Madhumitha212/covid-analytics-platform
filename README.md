# COVID-19 Global Analytics Platform
### (Spark + Hadoop + YARN | Distributed Data Engineering Project)

## Project Overview

This project simulates a real-world **Global Health Analytics Platform** built using:

- Apache Hadoop (HDFS + YARN)
- Apache Spark (PySpark)
- Spark SQL
- RDD API

The system processes large-scale COVID-19 datasets in a distributed environment using HDFS storage and Spark jobs running on YARN.

All data ingestion, processing, optimization, and analytics are performed in a pseudo-distributed Hadoop cluster.

---

## Architecture

Raw CSV Data  
⬇  
HDFS (/data/covid/raw)  
⬇  
Spark ETL (Schema + Cleaning)  
⬇  
Parquet (Staging Layer)  
⬇  
Analytics & Aggregations  
⬇  
Optimized Outputs (Parquet)

---

## Tech Stack

- Hadoop (HDFS + YARN)
- Apache Spark 3.x
- PySpark
- Spark SQL
- RDD API
- Parquet
- Kaggle COVID Dataset

---

## HDFS Directory Structure

/data/covid/raw
/data/covid/staging
/data/covid/curated
/data/covid/analytics

--

## Dataset Source

Kaggle Dataset:
https://www.kaggle.com/datasets/imdevskp/corona-virus-report

Tables Used:

- full_grouped.csv
- covid_19_clean_complete.csv
- country_wise_latest.csv
- day_wise.csv
- usa_county_wise.csv
- worldometer_data.csv

---

## Requirements & Setup
System Dependencies:

Java JDK 8 or above
Hadoop 3.x (HDFS + YARN)
Apache Spark 3.x with Hadoop support
Python Dependencies
Install Python packages using requirements.txt:
  pyspark==3.5.1

Clone Repository:

git clone https://github.com/Madhumitha212/covid-analytics-platform.git
cd covid-analytics-platform

## How to Run

Move to directory where the project exists

Setup Environment:
./scripts/setup.sh
./scripts/start.sh

Upload data to HDFS:

hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -put *.csv /data/covid/raw/

Run Spark Job:

spark-submit --master yarn pyspark_jobs/file_name.py
---

## Tasks Implemented

### Hadoop Integration
- Created HDFS directory structure
- Uploaded datasets
- Verified using HDFS commands

### Data Ingestion & Optimization
- Explicit schema used (no inferSchema)
- Null handling
- Converted CSV → Parquet
- Performance comparison (CSV vs Parquet)

### Death Percentage Analysis
- Daily country death %
- Global death %
- Continent-wise death %
- Top 10 countries by death rate

### Infection Rate Analysis
- Cases per 1000 population
- Active cases per 1000
- WHO region ranking

### Recovery Efficiency
- Recovery %
- 7-day rolling average (Window functions)
- Peak recovery day

### USA Drilldown
- County → State aggregation
- Top 10 states
- Data skew detection

### RDD Implementation
- reduceByKey usage
- Death percentage via RDD
- Performance comparison vs DataFrame

### Spark SQL Implementation
- Temporary views
- SQL-based analytics
- Execution plan comparison

### Performance Optimization
- Repartitioning strategy
- Broadcast joins
- Skew handling
- Shuffle optimization
- Caching strategy

---

## Performance Optimization Highlights

- Repartitioned by Date and Country
- Used BroadcastHashJoin for small lookup tables
- Tuned spark.sql.shuffle.partitions
- Implemented skew detection
- Used Parquet for columnar storage
- Reduced shuffle stages

---

## Execution Plan Analysis

Analyzed using:

df.explain("extended")

Observed:
- Exchange (Shuffle)
- BroadcastHashJoin
- SortMergeJoin
- WholeStageCodegen

---

## CSV vs Parquet Comparison

| Feature | CSV | Parquet |
|----------|------|---------|
| Storage | Row-based | Columnar |
| Size | Large | Compressed |
| Performance | Slower | Faster |
| Predicate Pushdown | No | Yes |
| Column Pruning | No | Yes |

Parquet significantly reduced I/O and improved execution time.

---

## Key Learnings

- Distributed data processing in Hadoop ecosystem
- Importance of shuffle optimization
- Handling data skew in Spark
- Broadcast join usage
- Partition strategy design
- Execution plan analysis
- Memory & resource planning in YARN

---

## Project Structure


covid-19-analytics-platform/
│
├── pyspark_jobs/
├── scripts/
├── datasets/
├── README.md
└── .gitignore

---

## Author

R Madhumitha

---