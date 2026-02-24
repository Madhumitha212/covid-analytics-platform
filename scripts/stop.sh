#!/bin/bash

echo "===== STOPPING COVID-19 ANALYTICS PLATFORM ====="

stop-yarn.sh
stop-dfs.sh
deactivate

echo "===== SERVICES STOPPED ====="