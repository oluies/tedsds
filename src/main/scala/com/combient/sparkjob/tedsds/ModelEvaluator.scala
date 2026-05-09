// scalastyle:off println
package com.combient.sparkjob.tedsds


import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD


object ModelEvaluator {


  def evaluatePrediction(predictionAndLabels: RDD[(Double, Double)], modelName: String = null): Unit = {

    val metrics: MulticlassMetrics = new MulticlassMetrics(predictionAndLabels)

    val metricsStrb = new StringBuilder(1024)
    metricsStrb append "************ "
    metricsStrb append modelName
    metricsStrb append "************ "
    metricsStrb append System.lineSeparator()
    metricsStrb append " |=================== Confusion matrix =========================="
    metricsStrb append System.lineSeparator()
    metricsStrb append metrics.confusionMatrix
    metricsStrb append System.lineSeparator()
    metricsStrb append " |=================== Confusion matrix =========================="

    val accuracy = metrics.accuracy
    metricsStrb append "\nSummary Statistics"
    metricsStrb append s"\nAccuracy = $accuracy"

    val labels = metrics.labels
    labels.foreach { l =>
      metricsStrb append (s"\nPrecision($l) = " + metrics.precision(l))
    }

    labels.foreach { l =>
      metricsStrb append (s"\nRecall($l) = " + metrics.recall(l))
    }

    labels.foreach { l =>
      metricsStrb append (s"\nFPR($l) = " + metrics.falsePositiveRate(l))
    }

    labels.foreach { l =>
      metricsStrb append (s"\nF1-Score($l) = " + metrics.fMeasure(l))
    }

    metricsStrb append s"\nWeighted precision: ${metrics.weightedPrecision}"
    metricsStrb append s"\nWeighted recall: ${metrics.weightedRecall}"
    metricsStrb append s"\nWeighted F1 score: ${metrics.weightedFMeasure}"
    metricsStrb append s"\nWeighted false positive rate: ${metrics.weightedFalsePositiveRate}"
    println("***")
    println(metricsStrb.toString())
  }
}
