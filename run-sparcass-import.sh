#!/usr/bin/env bash

# For development testing reasons, we need to reassembly when we change the code
# sbt assembly

if [ ! -f $1 ]; then
    echo "File not found! $1"
    exit -1
fi

spark-submit \
  --class "sparcass.GitHubLogsImporter" \
  --name "SparCass-Importer" \
  --master local \
  --conf "spark.driver.extraJavaOptions=
  -Dspark.app.cassandra.host=localhost
  -Dspark.app.cassandra.keyspace=test
  -Dspark.app.cassandra.table=glogs
  -Dspark.app.input.file=$1" \
  target/scala-2.10/learning-spark-fat.jar \
  spark.app.cassandra.host=localhost \
  spark.app.cassandra.keyspace=test \
  spark.app.cassandra.table=glogs \
  spark.app.input.file=$1