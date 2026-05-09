name := "tedsds"

version := "1.0"

scalaVersion := "2.13.14"

val sparkVersion = "3.5.1"

resolvers += "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"       % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive"      % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib"     % sparkVersion % Provided,
  "org.apache.spark" %% "spark-graphx"    % sparkVersion % Provided,
  "com.github.scopt" %% "scopt"           % "4.1.0"
)

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _                            => MergeStrategy.first
}
