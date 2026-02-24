#!/bin/bash

echo "===== STARTING COVID-19 ANALYTICS PLATFORM ====="

# Move to project root (one level up from scripts)
cd "$(dirname "$0")/.."

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Load environment variables
echo "Loading environment variables..."
source scripts/set_env.sh

# Unset Jupyter driver variables (important for YARN mode)
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

# Wait for services to initialize
sleep 5

echo "===== PROJECT STARTED ====="