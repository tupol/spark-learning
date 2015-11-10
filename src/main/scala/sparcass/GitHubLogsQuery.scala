package sparcass

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Small sample of querying Cassandra
 */
object GitHubLogsQuery {

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

    println("Show me some stuff!")

    val rt1 = timeCode {
      // Create an RDD corresponding to the Cassandra keyspace / table, selecting the the ev_table column
      // The `[String]' type in `cassandraTable[String]` will produce string results as opposed to CassandraRow results
      val types = sc.cassandraTable[String](props.cassandraKeyspace, props.cassandraTable).select("ev_type")

      // The `as(.....)` has a similar result as the above typed cassandraTable call
      //val types = sc.cassandraTable(props.cassandraKeyspace, props.cassandraTable).select("ev_type").as((t: String) => t)

      // The following will produce a list of CassandraRow objects (no typing, no .as(...) call)
      //val types = sc.cassandraTable(props.cassandraKeyspace, props.cassandraTable).select("ev_type")

      // Count the occurrences for each event type
      val groupedTypes = types.map((_, 1)).reduceByKey(_ + _)
      // Print grouped types
      println("Event types count using Spark aggregation:")
      groupedTypes.foreach(println)
    }
    println(s"Event types count using Spark aggregation runtime [millis]: $rt1")


    val rt2 = timeCode {
      // An alternative version of counting the event types, assuming that there is a relatively short list of distinct event types
      // !!!! From testing a few data samples this seems to be an anti-pattern
      // This version runs about 30-40% slower than the previous version
          def countByType(evType: String): Long =
            cqlContext.cassandraSql(s"SELECT COUNT(*) FROM test.glogs WHERE ev_type='$evType'").first.getLong(0)
          val distinctTypes = cqlContext.cassandraSql(s"SELECT DISTINCT(ev_type) FROM ${props.cassandraKeyspace}.${props.cassandraTable}")
          println("Event types count using separate Cassandra counts:")
          distinctTypes.map(_.getString(0)).collect.map(t => (t, countByType(t))).foreach(println)
    }
    println(s"Event types count using separate Cassandra counts runtime [millis]: $rt2")

    // Count all using the SQL Context
    val count = cqlContext.sql("SELECT count(*) FROM test.glogs")
    println(s"In the end we have ${count.first} records.")

    // Remove the table and the keyspace
    //    println("Cleaning up.")
    //    CassandraConnector(conf).withSessionDo { session =>
    //      session.execute("DROP TABLE test.glogs")
    //      session.execute("DROP KEYSPACE test")
    //    }

    println("Ok, we're done.")

  }

  /**
   * Run a block and return the runtime in millis
   * @param block
   * @return
   */
  def timeCode(block : => Unit): Long = {
    val start = new java.util.Date
    block
    val end = new java.util.Date
    end.toInstant.toEpochMilli - start.toInstant.toEpochMilli
  }

}
