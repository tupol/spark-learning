#!/usr/bin/env bash

# For development testing reasons, we need to reassembly when we change the code
# sbt assembly

spark-submit \
  --class "sparcass.GitHubLogsQuery" \
  --name "SparCass-Query" \
  --master local \
  target/scala-2.10/learning-spark-fat.jar \
  spark.app.cassandra.host=127.0.0.1 \
  spark.app.cassandra.keyspace=glogs \
  spark.app.cassandra.table.root=logs

#  --conf "spark.driver.extraJavaOptions=
#  -Dspark.app.cassandra.host=localhost
#  -Dspark.app.cassandra.keyspace=glogs
#  -Dspark.app.cassandra.table.root=logs" \