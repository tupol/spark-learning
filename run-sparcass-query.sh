#!/usr/bin/env bash

# For development testing reasons, we need to reassembly when we change the code
# sbt assembly

spark-submit \
  --class "sparcass.GitHubLogsQuery" \
  --name "SparCass-Query" \
  --master local \
  --conf "spark.driver.extraJavaOptions=
  -Dspark.app.cassandra.host=localhost
  -Dspark.app.cassandra.keyspace=test
  -Dspark.app.cassandra.table=glogs" \
  target/scala-2.10/learning-spark-fat.jar \
  spark.app.cassandra.host=localhost \
  spark.app.cassandra.keyspace=test \
  spark.app.cassandra.table=glogs
