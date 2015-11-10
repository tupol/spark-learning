# spark-learning
Various test/learning bits of code

## sparcass
Learning about the Spark-Cassandra integration

### Prerequisites

Install a local Cassandra DB, the simplest way possible:
unzip the targz in your environment.

### Importing GitHub logs data through Spark into Cassandra

 - `run-sparcass-import.sh /full/path/to/json/github/log/file.json` all around trivial script
 
 Inside the script one can configure a few parameters that are sent to the Spark driver.

### Doing a simple Cassandra query through Spark

 - `run-sparcass-query.sh`  
 
 Inside the script one can configure a few parameters that are sent to the Spark driver.

## spark
Learning about Spark... forgot what it was all about

Command line run: `run-spark.sh`