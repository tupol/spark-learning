package sparcass

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SimpleApp {

  def main(args: Array[String]) {

    //TODO Add some parameters for casandraHost and inputFile

    val appName = "SparCass"
    val cassandraHost = "localhost"
    val inputFile = "/Users/olivertupran/IdeaProjects/spark-learning/src/test/resources/glogs/2015-01-01-0.json"

    // Configures Spark.
    val conf = new SparkConf(true)
      .setAppName(appName)
      .set("spark.cassandra.connection.host", cassandraHost)

    // Connect to the Spark cluster
    val sc = new SparkContext(conf)

    //Create a Cassandra SQL context
    val cqlContext = new CassandraSQLContext(sc)


    // Create the keyspace and table using the manual connector.
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 };")
      // session.execute("CREATE TABLE IF NOT EXISTS test.glogs (id bigint PRIMARY KEY, type text);")
    }


    // Get the data from the input json file
    val importedDF = cqlContext.read.json(inputFile)

    //    importEventsDefault(importedDF.rdd, "test", "glogs")
    importEventsWithPKandCC(importedDF.rdd, "test", "glogs")


    // Count all using the SQL Context
    val count0 = cqlContext.sql("SELECT count(*) FROM test.glogs")
    println(s"Initially we have ${count0.first} records.")

    println("Show me the stuff!")


    // Create an RDD corresponding to the Cassandra keyspace / table
    val rdd = sc.cassandraTable("test", "glogs")

    // Count the RDDs
    println(s"We have ${rdd.count} rows.")

    // Print the row values
    rdd.collect.foreach(row => println(row.columnValues))

    // Select only the types
    val types = cqlContext.cassandraSql("SELECT ev_type from test.glogs")
    val groupedTypes = types.map((_, 1)).reduceByKey(_ + _)
    // Print grouped types
    groupedTypes.foreach(println)

    // Count all using the SQL Context
    val count1 = cqlContext.sql("SELECT count(*) FROM test.glogs")
    println(s"In the end we have ${count1.first} records.")


    println("Cleaning up.")

    // Remove the table and the keyspace
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("DROP TABLE test.glogs")
      session.execute("DROP KEYSPACE test")
    }

    println("Ok, we're done.")

  }

  /**
   * Import the events RDD (of rows) into the Cassandra database,
   * with minimal setup (by default partition key will be the `type` and
   * no clustering columns defined).
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
