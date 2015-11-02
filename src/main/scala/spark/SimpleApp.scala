package spark

import hadoop.custom.PatternRecordReader
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{InputSplit => MInputSplit, JobConf => MJobConf, RecordReader => MRecordReader, Reporter => MReporter, TextInputFormat => MTextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by oliver on 10/31/15.
 */
object SimpleApp {

  def main(args: Array[String]) {
    val rootPath = "src/test/resources/msweb"
    val dataPath = s"$rootPath/test.data" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val rawData =
        sc.newAPIHadoopFile [LongWritable, Text, CustomInputFormat2](dataPath)

    //    rawData.foreach(l => println(s"[[$l]]"))
    val data = rawData.map(x => s"---- ${x._1.toString} :: ${x._2.toString}").collect().foreach(println)
    //    val data = rawData.foreach(l => println(s"[[$l]]"))
    println(data)

//    val outPath = s"$rootPath/temp.data"
//
//    val outFile = new java.io.File(outPath)
//    if(outFile.exists())
//      outFile.delete()
//
//    rawData.map(x => x._2.toString).saveAsTextFile(outPath)

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
