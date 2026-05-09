package com.spotify.spark

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.compress.GzipCodec

object ImplicitALS {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ImplicitALS").getOrCreate()
    val sc = spark.sparkContext

    val input = args(1)
    val output = args(2)

    val ratings = sc.textFile(input)
      .map { l: String =>
        val t = l.split('\t')
        Rating(t(0).toInt, t(1).toInt, t(2).toFloat)
      }

    val model = ALS.trainImplicit(ratings, 40, 20, 0.8, 150)
    model
      .productFeatures
      .map { case (id, vec) =>
        s"$id\t${vec.map(d => "%.6f".format(d)).mkString(" ")}"
      }
      .saveAsTextFile(output, classOf[GzipCodec])

    spark.stop()
  }
}
