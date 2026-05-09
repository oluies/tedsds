/*
 * A simple example of a Spark job written in Scala.
 *
 * This tasks creates a table with arbitrary values,
 * distributes it over the nodes, writes it to HDFS
 * in parallel in two CSV and parquet (Spark optimized file format),
 * and print the number of lines in the table to the console.
 *
 *******************************************************
 *
 * Compilation: from the top folder (containing src/ and project/), run
 *    #sbt assembly
 *
 * Make sure the files this program is writing to do not exist
 * (otherwise, Spark will throw an error)
 *    #hadoop fs -rm -r -f SimpleExample.*
 *
 * Submit the job
 *    #spark-submit --class com.combient.sparkjob.SimpleExample --master yarn ./target/scala-2.13/tedsds-assembly-1.0.jar
 *
 *
 */

package com.combient.sparkjob

import org.apache.spark.sql.SparkSession

object SimpleExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SimpleExample").getOrCreate()
    import spark.implicits._

    val schema = Seq("id", "cykle", "value")
    val data = Seq(
      (1, 1, 1),
      (1, 2, 11),
      (1, 3, 1),
      (1, 4, 11),
      (1, 5, 1),
      (1, 6, 11),
      (2, 1, 1),
      (2, 2, 11),
      (2, 3, 1),
      (2, 4, 11),
      (2, 5, 1),
      (2, 6, 11))

    val dft = spark.sparkContext.parallelize(data).toDF(schema: _*)

    dft.write.parquet("SimpleExample.parquet")

    dft.write.option("header", "true").csv("SimpleExample.csv")

    println(s"############################## Count: ${dft.count()} ############################## ")

    spark.stop()
  }
}
