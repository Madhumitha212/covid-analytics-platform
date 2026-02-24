#!/bin/bash


#Java
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Hadoop
export HADOOP_HOME=/home/madhumitha/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Spark
export SPARK_HOME=$HOME/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# PySpark Notebook Settings
#export PYSPARK_DRIVER_PYTHON=jupyter
#export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

# Python path for PySpark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Local binaries
export PATH=$PATH:/home/madhumitha/.local/bin

