package sparcass

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object SimpleApp {

  def main(args: Array[String]) {

    // Configures Spark.
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setAppName("SparCass")

    // Connect to the Spark cluster:
    val sc = new SparkContext(conf)


    // Create the keyspace and table using the manual connector
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
      session.execute("CREATE TABLE test.words (word text PRIMARY KEY, count int);")
    }

    // Create an RDD corresponding to the Cassandra keyspace / table
    val rdd = sc.cassandraTable("test", "words")

    println("Show me the stuff!")

    // Add some data
    val collection = sc.parallelize(Seq(("key1", 1), ("key2", 2), ("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "words", SomeColumns("word", "count"))

    println(s"We have ${rdd.count} rows.")

    rdd.collect.foreach(println)

    println("Ok, I'm done.")


    // Remove the table and the keyspace
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("DROP TABLE test.words")
      session.execute("DROP KEYSPACE test")
    }

  }
}
