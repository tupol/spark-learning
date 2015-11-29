package cassandra

import java.util.Date

import com.datastax.driver.core.BoundStatement
import com.datastax.spark.connector.types.TimestampParser

import scala.collection.JavaConversions._
import scala.io.Source

/**
 *
 */
object GitHubLogsImporter extends App {

  // Load basic properties from the arguments
  val props: GitHubLogsProps = GitHubLogsArgsProps(args)

  println(
    s"Importing '${props.inputFile}' to cassandra://${props.cassandraHost}:${props.cassandraPort}/${props.cassandraKeyspace}.${props.cassandraTable}")

  val source = Source.fromFile(props.inputFile)

  def lines = source.getLines

  val cc = CassandraConnector.connect(props.cassandraHost, props.cassandraPort)

  println(s"Connected to cluster: ${cc.metadata.getClusterName}")
  println("Available nodes:")
  cc.metadata.getAllHosts.foreach(host => println(s" - Datatacenter: ${host.getDatacenter}; Host: ${host.getAddress}; Rack: ${host.getRack}"))

  //  cc.session.execute(createKeyspaceQuery)
  //  cc.session.execute(createTableQuery)

  cc.session.execute(
    """
      |select peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version
      |from system.peers;
    """.stripMargin)

  val statement = new BoundStatement(cc.session.prepare(
    s"""INSERT INTO ${props.cassandraKeyspace}.${props.cassandraTable}
       |(date_utc, hour_utc, created_at,
       |    ev_id, actor, ev_type, repo)
       |VALUES (?, ?, ?, ?, ?, ?, ?);
    """.stripMargin))

  val start = new java.util.Date
  val size = lines.map(lineToLogRecord).foldLeft(0) { (counter, record) =>
    cc.session.execute(
      statement.
        setString(0, record.date_utc).
        setInt(1, record.hour_utc).
        setDate(2, record.created_at).
        setLong(3, record.ev_id).
        setString(4, record.actor).
        setString(5, record.ev_type).
        setString(6, record.repo))
    counter + 1
  }
  val runtime = (new java.util.Date).toInstant.toEpochMilli - start.toInstant.toEpochMilli

  val speed = (size / (runtime / 1000.0))
  println("Stats:")
  println(f" - Processed $size lines in ${(runtime / 1000.0)}%.2f seconds")
  println(f" - Average speed: $speed%.2f records/second")

  cc.close


  val createKeyspaceQuery =
    """
      | CREATE KEYSPACE IF NOT EXISTS glogs WITH
      | replication = {'class': 'SimpleStrategy', 'replication_factor': '2'};
    """.stripMargin

  val createTableQuery =
    s"""
       |CREATE TABLE IF NOT EXISTS ${props.cassandraKeyspace}.${props.cassandraTable} (
       |    date_utc text,
       |    hour_utc int,
       |    created_at timestamp,
       |    ev_id bigint,
       |    actor text,
       |    ev_type text,
       |    repo text,
       |    PRIMARY KEY ((date_utc, hour_utc), created_at, ev_id)
       |) WITH CLUSTERING ORDER BY (created_at ASC, ev_id ASC);
    """.stripMargin


  case class LogRecord(ev_id: Long, created_at: Date, ev_type: String, actor: String, repo: String) {

    import java.time.ZoneId
    import java.time.format.DateTimeFormatter

    private val utcDateTime = created_at.toInstant.atZone(ZoneId.of("UTC"))
    val date_utc = utcDateTime.format(DateTimeFormatter.BASIC_ISO_DATE)
    val hour_utc = utcDateTime.getHour
  }

  def lineToLogRecord(line: String): LogRecord = {

    import org.json4s.DefaultFormats
    implicit val formats = DefaultFormats
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    val parsedLine = parse(line)
    val id = (parsedLine \ "id").extractOrElse("0").toLong
    val ev_type = (parsedLine \ "type").extractOrElse("NONE")
    val created_at = TimestampParser.parse((parsedLine \ "created_at").extractOrElse("1970-01-01T00:00:00Z"))
    val actor = (parsedLine \ "actor" \ "login").extractOrElse("NONE")
    val repo = (parsedLine \ "repo" \ "name").extractOrElse("NONE")
    LogRecord(id, created_at, ev_type, actor, repo)

  }

  //  println(createKeyspaceQuery)
  //  println(createTableQuery)


  /**
   * Run a block and return the runtime in millis
   * @param block
   * @return
   */
  def timeCode(block: => Unit): Long = {
    val start = new java.util.Date
    block
    val end = new java.util.Date
    end.toInstant.toEpochMilli - start.toInstant.toEpochMilli
  }

}
