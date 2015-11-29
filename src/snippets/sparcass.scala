spark-shell --jars ~/IdeaProjects/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.5.0-M2-SNAPSHOT.jar


import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._ //Imports basic rdd functions
import com.datastax.spark.connector.cql._ //(Optional) Imports java driver helper functions

val conf = new SparkConf(true).set("spark.driver.allowMultipleContexts", "true").set("spark.cassandra.connection.host", "localhost").setAppName("SparCass")

val sc = new SparkContext(conf)


CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 };")
  session.execute("CREATE TABLE test.words (word text PRIMARY KEY, count int);")
}

CassandraConnector(conf).withSessionDo { session =>
  session.execute("INSERT INTO test.words (word, count) VALUES ('abacus', 12);")
  session.execute("INSERT INTO test.words (word, count) VALUES ('cirrus', 23);")
}

val rdd = sc.cassandraTable("test", "words")
rdd.count


CassandraConnector(conf).withSessionDo { session =>
  session.execute("DROP TABLE test.words")
  session.execute("DROP KEYSPACE test")
}


import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._ //Imports basic rdd functions
import com.datastax.spark.connector.cql._ //(Optional) Imports java driver helper functions
import org.apache.spark.sql.cassandra._

sc.stop

val conf = new SparkConf(true).set("spark.driver.allowMultipleContexts", "true").set("spark.cassandra.connection.host", "localhost").setAppName("SparCass")

val sc = new SparkContext(conf)

val csqlc = new CassandraSQLContext(sc)

CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 };")
  session.execute("CREATE TABLE IF NOT EXISTS test.glogs (id bigint, type text, PRIMARY KEY(id, type));")
}

val dataFile = "/Users/olivertupran/IdeaProjects/glogs/2015-01-01-*.json"

val importedDF = csqlc.read.json(dataFile)
//
// case class EventByTypeActorNameId(evType: String, actorName: String, id: String, fullRecord: String)
//
// val events = importedDF.map(a => EventByTypeActorNameId(a(7).toString, a(0).toString.split(",")(3), a(2).toString, a.toString))
//
// events.saveAsCassandraTable("test", "glogs2", PartitionKeyColumns("eventType", "actorName", "id"))

import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef, ClusteringColumn, PartitionKeyColumn}
import com.datastax.spark.connector.types._

// Define structure for rdd data
case class EventByTypeActorNameId(ev_type:UUID, actor:UUID, id: Double, fullRecord:Int)

// Define columns
val p1Col = new ColumnDef("ev_type",PartitionKeyColumn,StringType)
val c1Col = new ColumnDef("actor",ClusteringColumn(0),StringType)
val c2Col = new ColumnDef("id",ClusteringColumn(1),LongType)
val rCol = new ColumnDef("fullRecord",RegularColumn,StringType)

// Create table definition
val table = TableDef("test","glogs",Seq(p1Col),Seq(c1Col, c2Col),Seq(rCol))

// Map rdd into custom data structure and create table
val events = importedDF.map(a => EventByTypeActorNameId(a(7).toString, a(0).toString.split(",")(3), a(2).toString, a.toString))
events.saveAsCassandraTableEx(table, SomeColumns("type", "actor", "id", "fullRecord"))


val toSave = importedDF.map(x => (x(2), x(7)))
toSave.saveToCassandra("test", "glogs", SomeColumns("id", "type"))


val cassandraRdd = sc.cassandraTable("test", "glogs")
cassandraRdd.take(10)

csqlc.sql("Select count(*) from test.glogs").first

CassandraConnector(conf).withSessionDo { session =>
  session.execute("DROP TABLE test.glogs")
  session.execute("DROP KEYSPACE test")
}

sc.cassandraTable("glogs", "logs_hourly_by_ts_id").
  select("actor").

// ==========================================


def getMostActivePerDateAndHour(fieldName: String, date_utc: String, hour_utc: Int): Option[(String, Int)] =
  sc.cassandraTable("glogs", "logs_hourly_by_ts_id").
    select(fieldName).
      where("date_utc=? and hour_utc=?", date_utc, hour_utc).
      map(x => (x.getString(0), 1)).
      reduceByKey(_ + _).
      sortBy(_._2, false).take(1).headOption

getMostActivePerDateAndHour("actor", "20150101Z", 1)


def getMostActivePerDateAndHours(fieldName: String, date_utc: String) = {
  val hours = (0 to 23)
  val hourlyActivity = hours.map(h => (h, getMostActivePerDateAndHour(fieldName, date_utc, h)))
  hourlyActivity.map(t => (t._1, t._2.take(1).headOption))
}

getMostActivePerDateAndHours("actor", "20150101Z")


def getFieldsPerDateAndHour(fieldName: String, date_utc: String, hour_utc: Int): RDD[CassandraRow] =
  sc.cassandraTable("glogs", "logs_hourly_by_ts_id").
    select(fieldName).
      where("date_utc=? and hour_utc=?", date_utc, hour_utc)


def getFieldsPerDate(fieldName: String, date_utc: String): RDD[CassandraRow] = {
  sc.cassandraTable("glogs", "logs_hourly_by_ts_id").
    select(fieldName).
      where("date_utc=? and hour_utc=?", date_utc, hour_utc)
}

def getMaxCount(rdd: RDD[CassandraRow]) =
  rdd.map(row => (row, 1)).
    reduceByKey(_ + _).
    sortBy(_._2, false).take(1).headOption
