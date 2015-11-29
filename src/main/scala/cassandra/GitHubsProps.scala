package cassandra

/**
 * Basic application properties
 *
 */

trait GitHubLogsProps {

  val cassandraHost: String
  val cassandraPort: Int
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

  val cassandraHost = argsMap.getOrElse("cassandra.host", "localhost")
  val cassandraPort = argsMap.getOrElse("cassandra.port", "9042").toInt
  val cassandraKeyspace = argsMap.getOrElse("cassandra.keyspace", "glogs")
  val cassandraTable = argsMap.getOrElse("cassandra.table.root", "logs")
  val inputFile = argsMap.getOrElse("input.file", "NoFile")

}


/**
 * Basic properties to be retrieved from the system properties.
 *
 * These properties are passed on to the driver by spark-submit through
 * --conf "spark.driver.extraJavaOptions=
 *    -Dcassandra.host=localhost
 *    ...."
 */
object GitHubLogsSysProps extends GitHubLogsProps {

  //TODO Add properties for replication configuration...

  val cassandraHost = sys.props.getOrElse("cassandra.host", "localhost")
  val cassandraPort = sys.props.getOrElse("cassandra.port", "9042").toInt
  val cassandraKeyspace = sys.props.getOrElse("cassandra.keyspace", "test")
  val cassandraTable = sys.props.getOrElse("cassandra.table", "glogs")
  val inputFile = sys.props.getOrElse("input.file", "NoFile")

}
