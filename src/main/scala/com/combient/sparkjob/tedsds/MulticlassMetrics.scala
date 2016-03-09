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
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, Row, SQLContext}

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object MulticlassMetricsFortedsds {

  case class Params(input: String = null)

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("MulticlassMetricsFortedsds") {
      head("MulticlassMetricsFortedsds: a http://spark.apache.org/docs/latest/mllib-linear-methods.html example app for ALS on dataset A. Saxena and K. Goebel (2008). “Turbofan Engine Degradation Simulation Data Set”, NASA Ames Prognostics Data Repository (http://ti.arc.nasa.gov/tech/dash/pcoe/prognostic-data-repository/), NASA Ames Research Center, Moffett Field, CA.")
      arg[String]("<input>")
        .optional()
        .text("hdfs input paths to a parquet dataset ")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a  dataset:
          |
          | bin/spark-submit --class com.combient.sparkjob.tedsds.MulticlassMetricsFortedsds \
          |  jarfile.jar \
          |  /share/tedsds/scaledd
        """.stripMargin)
    }


    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

  }

  def run(params: Params) {

    val conf = new SparkConf().setAppName(s"MulticlassMetricsExample with $params")
    val sc = new SparkContext(conf)

    val input = params.input
    //val output = args(2)
    println(s"Input dataset = $input")


    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // Load training data
    val scaledDF = sqlContext.read.parquet(input)


    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label2")
      .setOutputCol("indexedLabel")
      .fit(scaledDF)

    val indexed = labelIndexer.transform(scaledDF)

    val data : RDD[LabeledPoint] = indexed
      .select($"indexedLabel", $"scaledFeatures")
      .map{case Row(indexedLabel: Double, scaledFeatures: Vector) => LabeledPoint(indexedLabel, scaledFeatures)}


    // Split data into training (60%) and test (40%)
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(training)

    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Instantiate metrics object
    val metrics = new MulticlassMetrics(predictionAndLabels)

    // Confusion matrix
    val metricsStrb  = new StringBuilder(1024);
    metricsStrb append "Confusion matrix:"
    metricsStrb append metrics.confusionMatrix

    // Overall Statistics
    val precision = metrics.precision
    val recall = metrics.recall // same as true positive rate
    val f1Score = metrics.fMeasure
    metricsStrb append "\nSummary Statistics"
    metricsStrb append s"\nPrecision = $precision"
    metricsStrb append s"\nRecall = $recall"
    metricsStrb append s"\nF1 Score = $f1Score"

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      metricsStrb append (s"\nPrecision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      metricsStrb append (s"\nRecall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      metricsStrb append (s"\nFPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      metricsStrb append (s"\nF1-Score($l) = " + metrics.fMeasure(l))
    }

    // Weighted stats
    metricsStrb append (s"\nWeighted precision: ${metrics.weightedPrecision}")
    metricsStrb append (s"\nWeighted recall: ${metrics.weightedRecall}")
    metricsStrb append (s"\nWeighted F1 score: ${metrics.weightedFMeasure}")
    metricsStrb append (s"\nWeighted false positive rate: ${metrics.weightedFalsePositiveRate}")
    println("***")
    println(metricsStrb.toString())

    sc.stop()
  }
}
// scalastyle:on println
