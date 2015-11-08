#!/usr/bin/env bash

sbt assembly

spark-submit \
  --class "sparcass.SimpleApp" \
  --master local \
  target/scala-2.10/learning-spark-fat.jar