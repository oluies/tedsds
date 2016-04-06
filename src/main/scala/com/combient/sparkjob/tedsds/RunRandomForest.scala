/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.combient.sparkjob.tedsds

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser


case class Record(label: String, features: Vector)

object RunRandomForest {

  case class Params(input: String = null,model: String = null)

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("MulticlassMetricsFortedsds") {
      head("RunRandomForest: a http://spark.apache.org/docs/latest/mllib-linear-methods.html example app for ALS on dataset A. Saxena and K. Goebel (2008). “Turbofan Engine Degradation Simulation Data Set”, NASA Ames Prognostics Data Repository (http://ti.arc.nasa.gov/tech/dash/pcoe/prognostic-data-repository/), NASA Ames Research Center, Moffett Field, CA.")
      arg[String]("<input>")
        .required()
        .text("hdfs input paths to a parquet dataset ")
        .action((x, c) => c.copy(input = x.trim))
      arg[String]("<modelsave>")
        .optional()
        .text("hdfs output paths saved model ")
        .action((x, c) => c.copy(model = x.trim))
      note(
        """
          |For example, the following command runs this app on a  dataset:
          |
          | bin/spark-submit --class com.combient.sparkjob.tedsds.RunRandomForest \
          |  jarfile.jar \
          |  /share/tedsds/scaledd \
          |  /share/tedsds/savedmodel
        """.stripMargin)
    }


    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

  }

  def run(params: Params) {



    val conf = new SparkConf().setAppName(s"RunLogisticRegressionWithLBFGS with $params")
    val sc = new SparkContext(conf)

    val input = params.input
    println(s"Input dataset = $input")


    // see https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // Load training data
    val scaledDF = sqlContext.read.parquet(input)


    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label2")
      .setOutputCol("label")
      .fit(scaledDF)

    val indexed = labelIndexer.transform(scaledDF)

    val data : RDD[LabeledPoint] = indexed
      .select($"label", $"scaledFeatures")
      .map{case Row(label: Double, scaledFeatures: Vector) => LabeledPoint(label, scaledFeatures)}

    val trainRDD: RDD[Record] = data.map(lp => Record(lp.label.toString, lp.features))
    trainRDD.cache()

    val rf  = new RandomForestClassifier()
      .setNumTrees(3)
      .setFeatureSubsetStrategy("auto")
      .setImpurity("gini")
      .setMaxDepth(4)
      .setMaxBins(32)

    val pipeline = new Pipeline().setStages(Array(rf))

    val model = pipeline.fit(trainRDD.toDF())

    if(params.model != ""){
      model.save("%s".format(params.model))
      print("Saved model as %s".format(params.model))
    }

    sc.stop()
  }
}
// scalastyle:on println
