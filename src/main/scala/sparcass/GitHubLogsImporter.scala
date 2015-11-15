package sparcass

import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, TableDef, _}
import com.datastax.spark.connector.types.{TimestampParser, _}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Tool for importing GitHub log files into Cassandra
 */
object GitHubLogsImporter {

  val logger =  Logger.getLogger(GitHubLogsImporter.getClass)

  def main(args: Array[String]) {

    // Load basic properties from the arguments
    val props: GitHubLogsProps = GitHubLogsArgsProps(args)

    // Configures Spark.
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", props.cassandraHost)

    // Connect to the Spark cluster
    val sc = new SparkContext(conf)

    //Create a Cassandra SQL context
    val cqlContext = new CassandraSQLContext(sc)

    // Create the keyspace and table using the manual connector.
    // Normally this would already exist in the Cassandra cluster prior to the table creation, but for our purposes we create it here
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${props.cassandraKeyspace} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 };")
      // Yet another option for creating our table, using the manual connector
      // session.execute(s"CREATE TABLE IF NOT EXISTS ${props.cassandraKeyspace}.${props.cassandraTable} (id bigint PRIMARY KEY, type text);")
    }

    logger.info("Reading json file(s) into Spark...")
    // Get the data from the input json file and cache it, as we will be doing multiple saves
    val importedRDD = cqlContext.read.json(props.inputFile).rdd.cache

    val defaultTableExists = sc.cassandraTable("system", "schema_columns").
      where("keyspace_name=? and columnfamily_name=?",
        props.cassandraKeyspace, props.cassandraTable).
      count > 0

    logger.info("[Create table and] Import data into the default table (primary key = id).")
    if(defaultTableExists)
      importLogsDefault(importedRDD, props.cassandraKeyspace, props.cassandraTable)
    else
      createAndImportLogsDefault(importedRDD, props.cassandraKeyspace, props.cassandraTable)

    logger.info("[Create table and] Import data into the better structure table.")
    val betterTableName = props.cassandraTable + "_hourly_by_ts_id"
    val betterTableExists = sc.cassandraTable("system", "schema_columns").
      where("keyspace_name=? and columnfamily_name=?",
        props.cassandraKeyspace, betterTableName).
      count > 0

    if(betterTableExists)
      importHourlyLogsBy_CreatedAt_Id(importedRDD, props.cassandraKeyspace, betterTableName)
    else
      createAndImportHourlyLogsBy_CreatedAt_Id(importedRDD, props.cassandraKeyspace, betterTableName)

    // TODO Add some progress report, catch exceptions....

    logger.info("Ok, we're done.")

  }

  def importLogsDefault(rdd: RDD[Row], keyspaceName: String, tableName: String)(implicit converter: (Row) => LogRecordForImport): Unit = {

    // Map rdd into custom data structure and create table
    val events = rdd.map(converter)
    //TODO Figure out replication configuration
    events.saveToCassandra(keyspaceName, tableName)

  }

  /**
   * Import the events RDD (of rows) into the Cassandra database,
   * with minimal setup (by default partition key will be the `type` and
   * no clustering columns defined).
   * This creates a bit of a problem, since the type is not a good primary key
   *
   * @param rdd
   * @param keyspaceName
   * @param tableName
   * @param converter
   * @return
   */
  def createAndImportLogsDefault(rdd: RDD[Row], keyspaceName: String, tableName: String)(implicit converter: (Row) => LogRecordForImport): Unit = {

    // Map rdd into custom data structure and create table
    val events = rdd.map(converter)
    //TODO Figure out replication configuration
    events.saveAsCassandraTable(keyspaceName, tableName)

  }


  /**
   * Import the events RDD (of rows) into the Cassandra database,
   * using as partition key the UTC date and hour and as clustering columns
   * the "created_at" and "ev_id".
   * @param rdd
   * @param keyspaceName
   * @param tableName
   * @param converter
   * @return
   */
  def importHourlyLogsBy_CreatedAt_Id(rdd: RDD[Row], keyspaceName: String, tableName: String)(implicit converter: (Row) => LogRecordForImport): Unit = {

    // Map rdd into custom data structure and save the data
    val events = rdd.map(converter)
    events.saveToCassandra(keyspaceName, tableName,
      SomeColumns("date_utc", "hour_utc",
        "created_at", "ev_id",
        "repo", "actor", "ev_type" ))

  }


    /**
   * Create the proper table and import the events RDD (of rows) into the Cassandra database,
   * using as partition key the UTC date and hour and as clustering columns
   * the "created_at" and "ev_id".
   *
   *
   * @param rdd
   * @param keyspaceName
   * @param tableName
   * @param converter
   * @return
   */
  def createAndImportHourlyLogsBy_CreatedAt_Id(rdd: RDD[Row], keyspaceName: String, tableName: String)(implicit converter: (Row) => LogRecordForImport): Unit = {

    logger.info(s"Save data into Cassandra $keyspaceName.$tableName ...")
    logger.info("Partition key is date, hour and ev_type, actor, repo and id are clustering columns.")
    // TODO Think about what is the best partition key/primary key combination
    // Define columns
    // Partitioning columns
    val p1Col = new ColumnDef("date_utc", PartitionKeyColumn, VarCharType)
    val p2Col = new ColumnDef("hour_utc", PartitionKeyColumn, IntType)

    // Clustering columns
    val c1Col = new ColumnDef("created_at", ClusteringColumn(0), TimestampType)
    val c2Col = new ColumnDef("ev_id", ClusteringColumn(1), BigIntType)

    // Regular columns
    val r1Col = new ColumnDef("repo", RegularColumn, VarCharType)
    val r2Col = new ColumnDef("actor", RegularColumn, VarCharType)
    val r3Col = new ColumnDef("ev_type", RegularColumn, VarCharType)

    // Create table definition
    val table = TableDef(keyspaceName, tableName,
      Seq(p1Col, p2Col), // Partitioning columns
      Seq(c1Col, c2Col), // Clustering columns
      Seq(r1Col, r2Col, r3Col)) // Regular columns

    // Map rdd into custom data structure and create table
    val events = rdd.map(converter)
    events.saveAsCassandraTableEx(table,
      SomeColumns("date_utc", "hour_utc",
        "created_at", "ev_id",
        "repo", "actor", "ev_type" ))

  }

  /**
   * Define structure for the GitHub logs rdd data as it will be exported to Cassandra.
   * @param ev_id
   * @param created_at
   * @param ev_type
   * @param actor
   * @param repo
   */
  case class LogRecordForImport(ev_id: Long, created_at: Date, ev_type: String, actor: String, repo: String) {

    import java.time.ZoneId
    import java.time.format.DateTimeFormatter
    private val utcDateTime = created_at.toInstant.atZone(ZoneId.of("UTC"))
    val date_utc = utcDateTime.format(DateTimeFormatter.BASIC_ISO_DATE)
    val hour_utc = utcDateTime.getHour
  }

  /**
   * Implicit conversion between an SQL row and EventByTypeActorNameId
   *
   * Input row columns: Array(actor, created_at, id, org, payload, public, repo, type)
   * Sample input row: [ [https://avatars.githubusercontent.com/u/9152315?, ,9152315,davidjhulse,https://api.github.com/users/davidjhulse],2015-01-01T00:00:00Z,2489368070,null,[null,86ffa724b4d70fce46e760f8cc080f5ec3d7d85f,null,WrappedArray([ [david.hulse@live.com,davidjhulse],true,Altered BingBot.jar
   * Fixed issue with multiple account support,a9b22a6d80c1e0bb49c1cf75a3c075b642c28f81,https://api.github.com/repos/davidjhulse/davesbingrewardsbot/commits/a9b22a6d80c1e0bb49c1cf75a3c075b642c28f81]),null,1,null,a9b22a6d80c1e0bb49c1cf75a3c075b642c28f81,null,null,null,null,null,null,536740396,null,refs/heads/master,null,null,1],true,[28635890,davidjhulse/davesbingrewardsbot,https://api.github.com/repos/davidjhulse/davesbingrewardsbot],PushEvent]
   *
   * @param row
   * @return
   */
  implicit def sqlRowToEventByTypeActorNameId(row: Row): LogRecordForImport =
    LogRecordForImport(
      ev_id = row(2).toString.toLong,
      created_at = TimestampParser.parse(row(1).toString),
      ev_type = row(7).toString,
      actor = row(0).toString.split(",")(3),
      repo = row(6).toString.split(",")(1))

}
