//val logFile = "IdeaProjects/glogs/2015-02-28-7.json" // Should be some file on your system
val logFile = "IdeaProjects/glogs/*.json" // Should be some file on your system
val logData = sc.textFile(logFile, 2).cache()

val numAs = logData.filter(line => line.contains("a")).count()
val numBs = logData.filter(line => line.contains("b")).count()
println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

def extract(name: String)(input: String): Option[String] = {
  val pattern = s""""$name":"(\\w*)"""".r
  pattern.findFirstMatchIn(input).map(_.group(1))
}
def resultList = logData.map(extract("type")(_))
def resultsWithNones = resultList.map { case Some(x) => x; case None => "{NONE}" }
def groupedList = resultsWithNones.groupBy(p => p)
val rez = groupedList.map { case (k: String, v: List[String]) => (k, v.size) }

def extract(name: String)(input: String): Option[String] = {
  implicit val formats = DefaultFormats
  (parse(input) \ name).toSome.map(_.extract[String])
}


def extract(name: String)(input: String): Option[String] = {
  val pattern = s""""$name":"(\\w*)"""".r
  pattern.findFirstMatchIn(input).map(_.group(1))
}

val files = (new java.io.File("IdeaProjects/glogs/")).listFiles.filter(f => f.getName.endsWith(".json"))
val logDataPF = files.map(f => sc.textFile(f.getAbsolutePath))
val logData = sc.union(logDataPF)

def resultList = logData.map(extract("type")(_))
val rez = resultList.map(v => (v, 1)).reduceByKey((a, b) => a + b)

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.json("/Users/olivertupran/IdeaProjects/glogs/2015-01-01-*.json")

df.show()

df.select("name").show()




import sqlContext.implicits._

val interest = sc.makeRDD(List("DVSB", "MaxMillion", "ProjectDayDrum", "oliver")).toDF("name")

interest.registerTempTable("interest")

val query = """SELECT pt.actor.login, pt.type, count(*) as count
FROM interest ti LEFT JOIN pt ON (ti.name = pt.actor.login)
WHERE pt.type='ForkEvent'
GROUP BY actor.login, type
ORDER BY count desc
"""

sqlContext.sql(query).show


val query = """SELECT ti.name, pt.name, pt.type as count
FROM interest ti LEFT JOIN pt ON (ti.name = pt.actor.login)
--WHERE pt.type='ForkEvent'
GROUP BY ti.name, pt.actor.login, pt.type
ORDER BY count desc
"""

sqlContext.sql(query).show
