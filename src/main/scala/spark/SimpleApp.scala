package spark

import hadoop.custom.{PatternRecordReader, PatternInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{Reporter => MReporter, InputSplit => MInputSplit, TextInputFormat => MTextInputFormat, JobConf => MJobConf, RecordReader => MRecordReader}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, RecordReader, InputSplit}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
 * Created by oliver on 10/31/15.
 */
object SimpleApp {

  def main(args: Array[String]) {
//    val dataPath = "/home/oliver/IdeaProjects/learning-spark/src/test/resources/msweb/anonymous-msweb.data" // Should be some file on your system
    val dataPath = "/home/oliver/IdeaProjects/learning-spark/src/test/resources/msweb/test.data" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val rawData =
        sc.newAPIHadoopFile [LongWritable, Text, CustomInputFormat2](dataPath)

//    rawData.foreach(l => println(s"[[$l]]"))
val data = rawData.map(x => s"---- ${x._1.toString} :: ${x._2.toString}").collect().foreach(println)
//    val data = rawData.foreach(l => println(s"[[$l]]"))
    println(data)


  }

}

class CustomInputFormat2 extends FileInputFormat[LongWritable, Text] {

  override def createRecordReader(genericSplit: InputSplit,
                                  context: TaskAttemptContext): RecordReader[LongWritable, Text] = {

    val regex = "^[A,T,N,I,C],.*"
    context.getConfiguration.set("record.delimiter.regex", regex);

    return new PatternRecordReader
  }

}
