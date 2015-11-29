import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._




import org.apache.spark.streaming._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.annotation.tailrec
import scala.io.Source

import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(1))

// val fileName = "/Users/olivertupran/IdeaProjects/tmp/data/anonymous-msweb.data.txt"

val fileName = "/Users/olivertupran/IdeaProjects/tmp/data/temp.txt"

class CustomReceiver(filePath: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def isStartLine(line: String)(sos: List[String]): Boolean =
    sos.foldLeft(false)((acc, s) => acc || line.toUpperCase.startsWith(s))

  def filter(line: String) = isStartLine(line)(List("A", "C"))

  def action(lines: List[String]): Unit = {
    store(lines.mkString("\n"))
    println("---")
    println(lines.mkString("\n"))
  }

  @tailrec
  final def parseText(lines: Stream[String], acc: List[String], action : List[String] => Unit)(isStartLine : String => Boolean): Unit = lines match {
    case Stream.Empty => action(acc)
    case x #:: xs => {
      if (isStartLine(x)) {
        action(acc)
        parseText(xs, List(x), action)(isStartLine)
      }
      else {
        parseText(xs, x :: acc, action)(isStartLine)
      }
    }
  }

  override def onStart(): Unit = {
    println("### STARTED ###")
    val source = Source.fromFile(filePath)
    val lines = source.getLines().toStream
    parseText(lines, List(), action)(filter)
    source.close()
    // import java.io.File
    // val oldFile = new File(filePath)
    // val newFile = new File(filePath+".done")
    oldFile.renameTo(newFile)
    stop("Done!")
  }

  override def onStop(): Unit = {
    stop("Done!")
    println("### STOPPED ###")
  }
}

// import org.apache.spark.streaming._
//
// val fileName = "/Users/olivertupran/IdeaProjects/tmp/data/anonymous-msweb.data.txt"
//
// val ssc = new StreamingContext(sc, Seconds(1))


val crs = ssc.receiverStream(new CustomReceiver(fileName))

crs.map(x => "# " + x)
crs.print()

ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate
ssc.getState
ssc.stop()
