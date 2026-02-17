#!/bin/bash

echo COVID PROJECT ENV SETUP STARTED "

# Activate Virtual Environment
VENV_PATH="$HOME/projects/covid_analysis/venv"

if [ -d "$VENV_PATH" ]; then
    echo "Activating virtual environment at $VENV_PATH..."
    source $VENV_PATH/bin/activate
else
    echo "Virtual environment not found at $VENV_PATH. Please create it first."
    exit 1
fi

# Check & Set Java
echo "Checking Java installation..."
if command -v java >/dev/null 2>&1; then
    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo "Java already installed: $JAVA_VERSION"
else
    echo "Java not found. Installing Java 8..."
    sudo apt update -y
    sudo apt install openjdk-8-jdk -y
fi

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
echo "JAVA_HOME set to $JAVA_HOME"

# Check & Set Hadoop

echo "Checking Hadoop installation..."
if command -v hadoop >/dev/null 2>&1; then
    echo "Hadoop already installed"
    hadoop version | head -n 1
else
    echo "Hadoop not found. Installing Hadoop 3.3.6..."
    cd $HOME
    wget -q https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    tar -xzf hadoop-3.3.6.tar.gz
    mv hadoop-3.3.6 hadoop
fi

export HADOOP_HOME=$HOME/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Check & Set Spark

echo "Checking Spark installation..."
if command -v spark-submit >/dev/null 2>&1; then
    echo "Spark already installed"
    spark-submit --version | head -n 1
else
    echo "Spark not found. Installing Spark 3.5.1..."
    cd $HOME
    wget -q https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
    tar -xzf spark-3.5.1-bin-hadoop3.tgz
    mv spark-3.5.1-bin-hadoop3 spark
fi

export SPARK_HOME=$HOME/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Check & Install PySpark

echo "Checking PySpark installation..."
if python -c "import pyspark" >/dev/null 2>&1; then
    echo "PySpark already installed"
else
    echo "PySpark not found. Installing via pip..."
    pip install pyspark
fi

#  Set PySpark Notebook

export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

# Create Required HDFS Directories

echo "Creating HDFS directories for COVID project..."

hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -mkdir -p /data/covid/staging
hdfs dfs -mkdir -p /data/covid/analytics
hdfs dfs -chmod -R 777 /data/covid

# Optional: Spark logs directory
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod -R 777 /spark-logs

# Verify Installations

echo "Verifying installations..."

echo -n "Java Version: "
java -version 2>&1 | head -n 1

echo -n "Hadoop Version: "
hadoop version | head -n 1

echo -n "Spark Version: "
spark-submit --version | head -n 1

echo -n "PySpark Version: "
python -c "import pyspark; print(pyspark.__version__)"

echo " COVID PROJECT ENV SETUP COMPLETED "
