logLevel := Level.Warn
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.3")
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.5")


