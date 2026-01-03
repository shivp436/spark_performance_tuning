#!/bin/bash

echo "Submitting PySpark job to Spark cluster..."

docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark/work-dir/jobs/test.py

echo "Job submission completed!"
