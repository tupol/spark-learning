package sparcass

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Tool for importing GitHub log files into Cassandra
 */
object GitHubLogsImporter {

  def main(args: Array[String]) {

    import GitHubsProps._

    // Configures Spark.
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)

    // Connect to the Spark cluster
    val sc = new SparkContext(conf)

    //Create a Cassandra SQL context
    val cqlContext = new CassandraSQLContext(sc)

    // Create the keyspace and table using the manual connector.
    // Normally this would already exist in the Cassandra cluster prior to the table creation, but for our purposes we create it here
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $cassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 };")
      // Yet another option for creating our table, using hte manual connector
      // session.execute(s"CREATE TABLE IF NOT EXISTS $cassandraKeyspace.$cassandraTable (id bigint PRIMARY KEY, type text);")
    }

    println("Reading json file(s) into Spark...")
    // Get the data from the input json file
    val importedDF = cqlContext.read.json(inputFile)

    println("Save data into Cassandra...")
    importEventsWithPKandCC(importedDF.rdd, cassandraKeyspace, cassandraTable)

    // TODO Add some progress report, catch exceptions....

    println("Ok, we're done.")


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
  def importEventsDefault(rdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row], keyspaceName: String, tableName: String)(implicit converter: (org.apache.spark.sql.Row) => EventByTypeActorNameId): Unit = {

    // Map rdd into custom data structure and create table
    val events = rdd.map(converter)
    //TODO Figure out replication configuration
    events.saveAsCassandraTable(keyspaceName, tableName)

  }

  /**
   * Import the events RDD (of rows) into the Cassandra database,
   * using as partition key the `type` and as clustering columns
   * the actor name and event id.
   *
   * @param rdd
   * @param keyspaceName
   * @param tableName
   * @param converter
   * @return
   */
  def importEventsWithPKandCC(rdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row], keyspaceName: String, tableName: String)(implicit converter: (org.apache.spark.sql.Row) => EventByTypeActorNameId): Unit = {

    import com.datastax.spark.connector.cql.{ClusteringColumn, ColumnDef, PartitionKeyColumn, RegularColumn, TableDef}
    import com.datastax.spark.connector.types._

    // Define columns
    val p1Col = new ColumnDef("ev_type", PartitionKeyColumn, VarCharType)
    val c1Col = new ColumnDef("actor", ClusteringColumn(0), VarCharType)
    val c2Col = new ColumnDef("id", ClusteringColumn(1), BigIntType)
    val rCol = new ColumnDef("fullRecord", RegularColumn, VarCharType)

    // Create table definition
    val table = TableDef(keyspaceName, tableName, Seq(p1Col), Seq(c1Col, c2Col), Seq(rCol))

    // Map rdd into custom data structure and create table
    val events = rdd.map(converter)
    //TODO Figure out replication configuration
    events.saveAsCassandraTableEx(table, SomeColumns("ev_type", "actor", "id", "fullRecord"))

  }

  /**
   * Define structure for the GitHub logs rdd data as it will be exported to Cassandra.
   * @param ev_type
   * @param actor
   * @param id
   * @param fullRecord
   */
  case class EventByTypeActorNameId(ev_type: String, actor: String, id: Long, fullRecord: String)

  /**
   * Implicit conversion between an SQL row and EventByTypeActorNameId
   * @param row
   * @return
   */
  implicit def sqlRowToEventByTypeActorNameId(row: org.apache.spark.sql.Row): EventByTypeActorNameId = {
    EventByTypeActorNameId(row(7).toString, row(0).toString.split(",")(3), row(2).toString.toLong, row.toString)
  }

}
