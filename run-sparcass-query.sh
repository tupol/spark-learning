#!/usr/bin/env bash

sbt assembly

spark-submit \
  --class "sparcass.GitHubLogsQuery" \
  --name "SparCass-Query" \
  --master local \
  --conf "spark.driver.extraJavaOptions=
  -Dspark.app.cassandra.host=localhost
  -Dspark.app.cassandra.keyspace=test
  -Dspark.app.cassandra.table=glogs" \
  target/scala-2.10/learning-spark-fat.jar