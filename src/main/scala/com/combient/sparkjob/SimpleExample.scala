/*
 * A simple example of a job
 *
 *
 */

package com.combient.sparkjob

import org.apache.spark.sql.SQLContext

import org.apache.spark.{ SparkConf, SparkContext }

object SimpleExample {

  def main(args: Array[String]): Unit = {

    //Create a spark context
    val conf = new SparkConf().setAppName("SimpleExample")
    val sc = new SparkContext(conf)

    //Create some arbitrary data
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


    //This create a "spark dataframe", i.e. a table in the spark formalism, distributed over the nodes.
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dft = sc.parallelize(data).toDF(schema: _*)


    //Write the data into parquet format, a file format optimized for storing Spark dataframe
    dft.write.parquet("SimpleExample.parquet")

    //Write the data into standard CSV format
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
    dft.write.format("com.databricks.spark.csv").option("header", "true").save("SimpleExample.csv")


    println("############################## Count: " ${dft.count()} " ############################## ")

    sc.stop()

  }
}
