#!/usr/bin/env bash

sbt package

/home/oliver/apps/spark-1.5.1-bin-hadoop2.6/bin/spark-submit \
  --class "spark.SimpleApp" \
  --master local[4] \
  target/scala-2.11/learning-spark_2.11-1.0.jar