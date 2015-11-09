package sparcass

/**
 * Basic properties to be retrieved from the system properties.
 *
 * These propoerties are passed on to the driver by spark-submit through
 * --conf "spark.driver.extraJavaOptions=
 *    -Dspark.app.cassandra.host=localhost
 *    ...."
 */
object GitHubsProps {

  //TODO Add properties for replication configuration...

  val cassandraHost = sys.props.getOrElse("spark.app.cassandra.host", "localhost")
  val cassandraKeyspace = sys.props.getOrElse("spark.app.cassandra.keyspace", "test")
  val cassandraTable = sys.props.getOrElse("spark.app.cassandra.table", "glogs")
  val inputFile = sys.props.getOrElse("spark.app.input.file", "glogs")


}
