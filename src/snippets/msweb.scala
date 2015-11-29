import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

//---

//val fileName = "/Users/olivertupran/IdeaProjects/tmp/data/anonymous-msweb.data.txt"
val fileName = "/Users/olivertupran/IdeaProjects/spark-learning/src/test/resources/msweb/test.data"


val data = sc.textFile(fileName, 2).cache()

val sdata = data.map(line => line.split(","))

val eventsOfA = sdata.filter(_(0) == "A")
val eventsOfC = sdata.filter(_(0) == "C")
val eventsOfV = sdata.filter(_(0) == "V")

val eventsOfCandV = sdata.filter(a => a(0) == "V" || a(0) == "C")


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// val df = sqlContext.read.

eventsOfCandV.foldLeft(List[String, String]())((acc, x) => {
  x(0) match {
    case "C" => (x(1), List()) + acc
    case "V" => ()
  }

type InputType = Array[Array[String]]
type ResultType = Array[(Array[String], Array[String]

  def fn(input: InputType, acc: ResultType) : ResultType = input match {
    case Nil => acc
    case x :: xs => if(acc.first)
  }



  import org.apache.hadoop.io.{LongWritable, Text}
  import org.apache.hadoop.mapred.{TextInputFormat, InputSplit, RecordReader, JobConf, Reporter, TaskAttemptContext}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  class CustomInputFormat extends TextInputFormat {

    override def getRecordReader(genericSplit: InputSplit,
                                 job: JobConf,
                                 reporter: Reporter): RecordReader[LongWritable, Text] = {
      job.set("textinputformat.record.delimiter", "\nC")
      super.getRecordReader(genericSplit, job, reporter)
    }
  }
  val dataPath = "/Users/olivertupran/IdeaProjects/tmp/data/temp.txt"

  val rawData: RDD[(LongWritable, Text)] = sc.hadoopFile[LongWritable, Text, CustomInputFormat](dataPath)
  rawData.take(10).collect









class PatternInputFormat extends FileInputFormat[LongWritable,Text] {

  override def createRecordReader(split : InputSplit, context: TaskAttemptContext ) : RecordReader[LongWritable, Text] = {
       new PatternRecordReader()
  }

}
