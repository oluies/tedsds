package com.spotify.spark


import org.apache.spark._
import org.apache.spark.mllib.recommendation.{Rating, ALS}
import org.apache.hadoop.io.compress.GzipCodec

object ImplicitALS {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleExample")

    val sc = new SparkContext(conf)
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
        id + "\t" + vec.map(d => "%.6f".format(d)).mkString(" ")
      }
      .saveAsTextFile(output, classOf[GzipCodec])

    sc.stop()
  }
}