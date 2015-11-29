#!/usr/bin/env bash

# For development testing reasons, we need to reassembly when we change the code
# sbt assembly

if [ ! -f "$1" ]; then
    echo "File not found! $1"
    exit -1
fi

java  \
  -cp target/scala-2.10/learning-spark-fat.jar \
  cassandra.GitHubLogsImporter \
  cassandra.host=192.168.100.10 cassandra.keyspace=glogs cassandra.table.root=logs1 input.file=$1


#find /Users/olivertupran/IdeaProjects/glogs/2015-01-02-*.json  | xargs -I '{}' ./run-cassandra-import.sh {}
