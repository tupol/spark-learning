package spark

import hadoop.custom.PatternRecordReader
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by oliver on 10/31/15.
 */
object SimpleApp {

  case class Attribute(id: Int, title: String, url: String)
  case class Case(id: Int, attrIds: List[Int])

  def main(args: Array[String]) {

    val rootPath = "src/test/resources/msweb"
//    val dataPath = s"$rootPath/test.data" // Should be some file on your system
    val dataPath = s"$rootPath/anonymous-msweb.data" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val rawData =
        sc.newAPIHadoopFile [LongWritable, Text, CustomInputFormat2](dataPath)

    val data = rawData.map(line => line._2.toString).map(l => l.split("\\{"))

    val sdata = data.map(arr => {
      val splits = arr(0).split(",")
      if(arr.size > 1)
        (splits.toList :+ arr(1)).toArray
      else splits
    })

    val attrs = sdata.filter(_(0) == "A").map(a => Attribute(a(1).toInt, a(3), a(4))).map(a => (a.id, a))

    val cases = sdata.filter(_(0) == "C").map(a => {
      if(a.length >= 4) {
//        println(a.length + " / " + a.size + " / " + a.toList)
        val pattern = """V,(\d*),\d*""".r
        val rez = pattern.findAllIn(a(3))
        val votes = rez.map(_ => rez.group(1)).toList.map(_.toInt)
//        println( "  " + votes)
        Case(a(2).toInt, votes)
      } else {
        Case(a(2).toInt, List())
      }
    })

    attrs.take(10).foreach(println)
    cases.filter(c => c.attrIds.size > 0).take(10).foreach(println)
    val votes = cases.flatMap(c => c.attrIds).map(a => (a, 1)).reduceByKey((a, b) => a + b)

    votes.take(20).foreach(println)

    val result = attrs.leftOuterJoin(votes)


//    type Result = Tuple2[Int, Tuple2[Attribute, Option[Int]]]
//    implicit object ResultOrdering extends Ordering[Result] {
//      def compare(a:Result, b:Result) = {
//        val x = a._2._2.getOrElse(0).toInt
//        val y = b._2._2.getOrElse(0).toInt
//        x compareTo y
//      }
//    }

    result.take(20).foreach(println)


  }

}

class CustomInputFormat2 extends FileInputFormat[LongWritable, Text] {

  override def createRecordReader(genericSplit: InputSplit,
                                  context: TaskAttemptContext): RecordReader[LongWritable, Text] = {

    val regex = "^[A,N,T,I,C],.*"
    context.getConfiguration.set("record.delimiter.regex", regex);

    return new PatternRecordReader
  }

}
