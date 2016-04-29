// scalastyle:off println
package com.combient.sparkjob.tedsds


import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object ModelEvaluator {


  def evaluatePrediction( predictionAndLabels : RDD[(Double, Double)] , modelName : String = null) : Unit = {


    // Instantiate metrics object
    val metrics: MulticlassMetrics = new MulticlassMetrics(predictionAndLabels)

    // Confusion matrix
    val metricsStrb  = new StringBuilder(1024)
    metricsStrb append "************ "
    metricsStrb append modelName
    metricsStrb append "************ "
    metricsStrb append System.getProperty("line.separator")
    metricsStrb append " |=================== Confusion matrix =========================="
    metricsStrb append System.getProperty("line.separator")
    metricsStrb append metrics.confusionMatrix
    metricsStrb append System.getProperty("line.separator")
    metricsStrb append " |=================== Confusion matrix =========================="


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
    metricsStrb append s"\nWeighted precision: ${metrics.weightedPrecision}"
    metricsStrb append s"\nWeighted recall: ${metrics.weightedRecall}"
    metricsStrb append s"\nWeighted F1 score: ${metrics.weightedFMeasure}"
    metricsStrb append s"\nWeighted false positive rate: ${metrics.weightedFalsePositiveRate}"
    println("***")
    println(metricsStrb.toString())


  }
}
