name := "spark-learning-scala"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.scala-lang" % "scala-reflect" % "2.10.6",
  "org.scala-lang" % "scala-compiler" % "2.10.6",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0-M2"
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}