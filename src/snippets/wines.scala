import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object wines extends App {

  // Configures Spark.
  val conf = new SparkConf(true)

  // Connect to the Spark cluster
  val sc = new SparkContext(conf)

  val sqc = new org.apache.spark.sql.SQLContext(sc)

  val fileName = "/Users/olivertupran/IdeaProjects/spark-learning/src/test/resources/wines/winequality-white.csv"
  // val fileName = "/Users/olivertupran/IdeaProjects/spark-learning/src/test/resources/wines/winequality-red.csv"

  val delimiter = ";"
  val rawData = sc.textFile(fileName, 2).
    map(line => line.split(delimiter).toSeq)

  val header = rawData.first.toSeq
  val data = rawData.filter(_ != header)

  data.count - data.distinct.count

  val lpoints = data.take(100).
    map(p => LabeledPoint(p.last.toDouble,
      Vectors.dense(p.dropRight(1).map(_.toDouble).toArray)))

}