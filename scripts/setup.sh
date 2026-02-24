#!/bin/bash

echo "===== SETUP STARTED ====="

sudo apt update

# Install Java if missing
if ! command -v java &> /dev/null
then
    echo "Installing OpenJDK 8..."
    sudo apt install openjdk-8-jdk -y
else
    echo "Java already installed."
fi

# Install python venv if missing
if ! dpkg -s python3-venv &> /dev/null
then
    sudo apt install python3-venv -y
fi

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists."
fi

# Activate venv
source venv/bin/activate

# Install pyspark only if not present
if ! pip show pyspark &> /dev/null
then
    echo "Installing pyspark 3.5.1..."
    pip install pyspark==3.5.1
else
    echo "pyspark already installed."
fi

deactivate

echo "===== SETUP COMPLETED ====="