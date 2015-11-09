package sparcass

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Small sample of querying Cassandra
 */
object GitHubLogsQuery {

  def main(args: Array[String]) {

    import GitHubsProps._

    // Configures Spark.
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)

    // Connect to the Spark cluster
    val sc = new SparkContext(conf)

    //Create a Cassandra SQL context
    val cqlContext = new CassandraSQLContext(sc)

    println("Show me some stuff!")

    // Create an RDD corresponding to the Cassandra keyspace / table, selecting the the ev_table column
    val types = sc.cassandraTable(cassandraKeyspace, cassandraTable).select("ev_type")

    // Count the occurrences for each event type
    val groupedTypes = types.map((_, 1)).reduceByKey(_ + _)
    // Print grouped types
    groupedTypes.foreach(println)

    // Count all using the SQL Context
    val count1 = cqlContext.sql("SELECT count(*) FROM test.glogs")
    println(s"In the end we have ${count1.first} records.")

    // Remove the table and the keyspace
    //    println("Cleaning up.")
    //    CassandraConnector(conf).withSessionDo { session =>
    //      session.execute("DROP TABLE test.glogs")
    //      session.execute("DROP KEYSPACE test")
    //    }

    println("Ok, we're done.")

  }


}
