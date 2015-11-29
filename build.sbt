name := "learning-spark"

version := "1.0"

scalaVersion := "2.10.4"


mainClass in Compile := Some("sparcass.SimpleApp")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M2" withSources() withJavadoc()

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1"


run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblyJarName in assembly := s"${name.value}-fat.jar"

// Add exclusions, provided...
assemblyMergeStrategy in assembly := {
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
