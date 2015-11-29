#!/usr/bin/env bash

# For development testing reasons, we need to reassembly when we change the code
# sbt assembly

# if [ ! -f "$1" ]; then
#     echo "File not found! $1"
#     exit -1
# fi

spark-submit \
  --class "sparcass.GitHubLogsImporter" \
  --name "SparCass-Importer" \
  --master spark://192.168.100.10:7077 \
  target/scala-2.10/learning-spark-fat.jar \
  spark.app.cassandra.host=192.168.100.10 \
  spark.app.cassandra.keyspace=glogs \
  spark.app.cassandra.table.root=logs \
  spark.app.input.file=$1

# Alternatively we can send parameters through the extra java options
#  --conf "spark.driver.extraJavaOptions=
#  -Dspark.app.cassandra.host=localhost
#  -Dspark.app.cassandra.keyspace=glogs
#  -Dspark.app.cassandra.table.root=logs
#  -Dspark.app.input.file=$1" \
