package sparcass

import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.IndexedSeq


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

    println("--------------------------------------------------")
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


    println("--------------------------------------------------")
    val rt2 = timeCode {
      // An alternative version of counting the event types, assuming that there is a relatively short list of distinct event types
      // !!!! From testing a few data samples this seems to be an anti-pattern
      // This version runs about 30-40% slower than the previous version
          def countByType(evType: String): Long =
            cqlContext.cassandraSql(s"SELECT COUNT(*) FROM ${props.cassandraKeyspace}.${props.cassandraTable} WHERE ev_type='$evType'").first.getLong(0)
          val distinctTypes = cqlContext.cassandraSql(s"SELECT DISTINCT(ev_type) FROM ${props.cassandraKeyspace}.${props.cassandraTable}")
          println("Event types count using separate Cassandra counts:")
          distinctTypes.map(_.getString(0)).collect.map(t => (t, countByType(t))).foreach(println)
    }
    println(s"Event types count using separate Cassandra counts runtime [millis]: $rt2")

    println("--------------------------------------------------")
    // Count all using the SQL Context
    val count = cqlContext.sql(s"SELECT count(*) FROM ${props.cassandraKeyspace}.${props.cassandraTable}")
    println(s"In the end we have ${count.first} records.")


    def getMaxCountPerDateAndHour(fieldName: String, date_utc: String, hour_utc: Int): Option[(String, Int)] =
      sc.cassandraTable[CassandraRow](props.cassandraKeyspace, props.cassandraTable + "_hourly_by_ts_id").
        select(fieldName).
        where("date_utc=? and hour_utc=?", date_utc, hour_utc).
        map(x => (x.getString(0), 1)).
        reduceByKey(_ + _).
        sortBy(_._2, false).take(1).headOption


    def getMaxCountPerDateAndHours(fieldName: String, date_utc: String) = {
      val hours = (0 to 23)
      val hourlyActivity = hours.map(h => (h, getMaxCountPerDateAndHour(fieldName, date_utc, h)))
      hourlyActivity.map(t => (t._1, t._2.take(1).headOption))
    }


    val date_utc_1 = "20150101Z"
    println("--------------------------------------------------")
    println(s"Most active actors per hour on $date_utc_1")
    println(getMaxCountPerDateAndHours("actor", date_utc_1).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active types per hour on $date_utc_1")
    println(getMaxCountPerDateAndHours("ev_type", date_utc_1).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active repos per hour on $date_utc_1")
    println(getMaxCountPerDateAndHours("repo", date_utc_1).mkString("\n"))

    val date_utc_2 = "20150102Z"
    println("--------------------------------------------------")
    println(s"Most active actors per hour on $date_utc_2")
    println(getMaxCountPerDateAndHours("actor", date_utc_2).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active types per hour on $date_utc_2")
    println(getMaxCountPerDateAndHours("ev_type", date_utc_2).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active repos per hour on $date_utc_2")
    println(getMaxCountPerDateAndHours("repo", date_utc_2).mkString("\n"))


    def getFieldsPerDateAndHour(fieldName: String, date_utc: String, hour_utc: Int): RDD[CassandraRow] =
      sc.cassandraTable("glogs", "logs_hourly_by_ts_id").
        select(fieldName).
        where("date_utc=? and hour_utc=?", date_utc, hour_utc)


    def getFieldsPerDate(fieldName: String, date_utc: String): RDD[CassandraRow] = {
      val hours = (0 to 23)
      val hourlyActivity = hours.map(h => getFieldsPerDateAndHour(fieldName, date_utc, h))
      hourlyActivity.reduce(_ union _)
    }

    def getMaxCountByRow(rdd: RDD[CassandraRow]): Option[(CassandraRow, Int)] =
      rdd.map(row => (row, 1)).
        reduceByKey(_ + _).
        sortBy(_._2, false).take(1).headOption

    def getMaxCountByRowPerDateAndHours(fieldName: String, date_utc: String): IndexedSeq[(Int, Option[(CassandraRow, Int)])] = {
      val hours = (0 to 23)
      hours.map(h => (h, getMaxCountByRow(getFieldsPerDateAndHour(fieldName, date_utc, h))))
    }

    println("--------------------------------------------------")
    println(s"Most active actors per hour on $date_utc_1")
    println(getMaxCountByRowPerDateAndHours("actor", date_utc_1).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active types per hour on $date_utc_1")
    println(getMaxCountByRowPerDateAndHours("ev_type", date_utc_1).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active repos per hour on $date_utc_1")
    println(getMaxCountByRowPerDateAndHours("repo", date_utc_1).mkString("\n"))

    println("--------------------------------------------------")
    println(s"Most active actors per hour on $date_utc_2")
    println(getMaxCountByRowPerDateAndHours("actor", date_utc_2).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active types per hour on $date_utc_2")
    println(getMaxCountByRowPerDateAndHours("ev_type", date_utc_2).mkString("\n"))
    println("--------------------------------------------------")
    println(s"Most active repos per hour on $date_utc_2")
    println(getMaxCountByRowPerDateAndHours("repo", date_utc_2).mkString("\n"))


    // Remove the table and the keyspace
    //    println("Cleaning up.")
    //    CassandraConnector(conf).withSessionDo { session =>
    //      session.execute(s"DROP KEYSPACE ${props.cassandraKeyspace}")
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
