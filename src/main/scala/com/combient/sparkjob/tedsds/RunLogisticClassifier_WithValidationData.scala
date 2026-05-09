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
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser

object RunLogisticRegressionWithValidationData {

  case class Params(input: String = null, model: String = null, label: Int = 2)

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("MulticlassMetricsFortedsds") {
      head("RunLogisticRegressionWithLBFGS")
      arg[String]("<input>")
        .required()
        .text("hdfs input paths to a parquet dataset ")
        .action((x, c) => c.copy(input = x.trim))
      arg[String]("<modelsave>")
        .optional()
        .text("hdfs output paths saved model ")
        .action((x, c) => c.copy(model = x.trim))
      opt[Int]('l', "label")
        .optional()
        .action { (x, c) => c.copy(label = x) }
        .text("What label to use")
      note(
        """
          |For example, the following command runs this app on a  dataset:
          |
          | bin/spark-submit --class com.combient.sparkjob.tedsds.RunLogisticRegressionWithLBFGS \
          |  jarfile.jar \
          |  /share/tedsds/scaledd \
          |  /share/tedsds/savedmodel
          |  --label  1/2 (which label to use)
        """.stripMargin)
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case None         => System.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession.builder()
      .appName(s"RunLogisticRegressionWithValidationData with $params")
      .getOrCreate()
    val sc = spark.sparkContext

    val (labeltouse, nbClasses) =
      if (params.label == 1) ("label1", 2) else ("label2", 3)

    println("Logistic regression with validation data")
    println(s"Input dataset = ${params.input}")
    println(s"Output file = ${params.model}")
    println(s"Using $labeltouse")

    val scaledDF = spark.read.parquet(params.input)

    val labelIndexer = new StringIndexer()
      .setInputCol(labeltouse)
      .setOutputCol("indexedLabel")
      .fit(scaledDF)

    val indexed = labelIndexer.transform(scaledDF)

    import spark.implicits._
    val dataRDD: RDD[LabeledPoint] = indexed
      .select($"indexedLabel", $"scaledFeatures")
      .rdd
      .map { case Row(indexedLabel: Double, scaledFeatures: Vector) =>
        LabeledPoint(indexedLabel, scaledFeatures)
      }

    val Array(training, validation) = dataRDD.randomSplit(Array(0.80, 0.20), seed = 11L)
    training.cache()

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(nbClasses)
      .run(training)

    val predictionAndLabelsTrain = training.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val predictionAndLabelsValidation = validation.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    if (params.model != null && params.model != "") {
      model.save(sc, "%s".format(params.model))
      print("Saved model as %s".format(params.model))
    }

    ModelEvaluator.evaluatePrediction(predictionAndLabelsTrain, "Logistic Classifier --- Training set")
    ModelEvaluator.evaluatePrediction(predictionAndLabelsValidation, "Logistic Classifier --- Validation set")

    spark.stop()
  }
}
// scalastyle:on println
