package sparcass

/**
 * Basic application properties
 *
 */

trait GitHubLogsProps {

  val cassandraHost: String
  val cassandraKeyspace: String
  val cassandraTable: String
  val inputFile: String
}


/**
 * Basic properties to be retrieved from the application arguments.
 *
 * These properties are passed on to the driver by spark-submit on the last line
 */
case class GitHubLogsArgsProps(args: Array[String]) extends GitHubLogsProps {

  //TODO Add properties for replication configuration...

  val argsMap = args.map(_.split("=")).map(x => (x(0), x(1))).toMap

  val cassandraHost = argsMap.getOrElse("spark.app.cassandra.host", "localhost")
  val cassandraKeyspace = argsMap.getOrElse("spark.app.cassandra.keyspace", "test")
  val cassandraTable = argsMap.getOrElse("spark.app.cassandra.table", "glogs")
  val inputFile = argsMap.getOrElse("spark.app.input.file", "NoFile")

}


/**
 * Basic properties to be retrieved from the system properties.
 *
 * These properties are passed on to the driver by spark-submit through
 * --conf "spark.driver.extraJavaOptions=
 *    -Dspark.app.cassandra.host=localhost
 *    ...."
 */
object GitHubLogsSysProps extends GitHubLogsProps {

  //TODO Add properties for replication configuration...

  val cassandraHost = sys.props.getOrElse("spark.app.cassandra.host", "localhost")
  val cassandraKeyspace = sys.props.getOrElse("spark.app.cassandra.keyspace", "test")
  val cassandraTable = sys.props.getOrElse("spark.app.cassandra.table", "glogs")
  val inputFile = sys.props.getOrElse("spark.app.input.file", "NoFile")

}
