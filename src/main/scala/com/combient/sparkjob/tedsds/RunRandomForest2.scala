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

import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser



object RunRandomForest2 {

  case class Params(input: String = null,model: String = null,label: Int = 2)

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
      opt[Int]('l',"label")
        .optional()
        .action { (x, c) => c.copy(label = x) }
        .text("What label to use")
      note(
        """
          |For example, the following command runs this app on a  dataset:
          |
          | bin/spark-submit --class com.combient.sparkjob.tedsds.RunRandomForest2 \
          |  jarfile.jar \
          |  /share/tedsds/scaledd \
          |  /share/tedsds/savedmodel
          |   --label  1/2 (which label to use)
        """.stripMargin)
    }


    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

  }

  def run(params: Params) {

    //Start Spark context
    val conf = new SparkConf().setAppName(s"RunRandomForest2 with $params")
    val sc = new SparkContext(conf)


    //Chose which label to use
    var labeltouse : String = "label2"
    var nbClasses : Int = 3

    if (params.label == 1 ){
        labeltouse = "label1"
        nbClasses = 2
    }
    //Print information about the model
    val input = params.input
    val output = params.model
    println("Random forest 2 ")
    println(s"Input dataset = $input")
    println(s"Output file = $output")
    println(s"Using $labeltouse")

    // see https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // Load training data
    val scaledDF = sqlContext.read.parquet(input)

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol(labeltouse)
      .setOutputCol("indexedLabel")
      .fit(scaledDF)

    val indexed = labelIndexer.transform(scaledDF)

    //Create an RDD suitable for the ML algorithm
    val trainRDD : RDD[LabeledPoint] = indexed
      .select($"indexedLabel", $"scaledFeatures")
      .map{case Row(indexedLabel: Double, scaledFeatures: Vector) => LabeledPoint(indexedLabel, scaledFeatures)}

    //Tell Spark to keep the data in memory
    trainRDD.cache()

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = nbClasses // 0-2
    val categoricalFeaturesInfo = Map[Int, Int]()   //
    val numTrees = 666
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 6
    val maxBins = 32

    // Train the model
    val model: RandomForestModel = RandomForest.trainClassifier(trainRDD, numClasses,
      categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Predict the labels of the training data
    val predictionAndLabels = trainRDD.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Save the model
    if(params.model != ""){
      model.save(sc,"%s".format(params.model))
      print("Saved model as %s".format(params.model))
    }


    // Evaluate the model
    ModelEvaluator.evaluatePrediction(predictionAndLabels,"Random forest (#tree=666 , depth=6) --- Training set")


    sc.stop()
  }
}
// scalastyle:on println
