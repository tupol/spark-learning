#!/usr/bin/env bash

sbt package

spark-submit \
  --class "spark.SimpleApp" \
  --master local \
  target/scala-2.11/learning-spark_2.11-1.0.jar