name := "tedsds"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

sparkVersion := "1.6.0"

sparkComponents ++= Seq("streaming", "sql","mllib","graphx","hive")

spDependencies += "com.databricks/spark-csv_2.10:1.4.0"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Hortonworks Releases" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= {
  val akkaV = "2.3.0"
  val sprayV = "1.3.1"
  Seq(
    "com.github.scopt" %% "scopt" % "3.4.0"
  )
}
