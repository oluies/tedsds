package com.combient.sparkjob.tedsds

/**
  * Created by olu on 09/03/16.
  */

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import scopt.OptionParser

import scala.collection.immutable.IndexedSeq


class PrepareData {
  /*
    This function defines the schema of the input data.
   */
  def getSchema: StructType = {
    StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("cycle", IntegerType, nullable = false),
      StructField("setting1", DoubleType, nullable = true),
      StructField("setting2", DoubleType, nullable = true),
      StructField("setting3", DoubleType, nullable = true),
      StructField("s1", DoubleType, nullable = true),
      StructField("s2", DoubleType, nullable = true),
      StructField("s3", DoubleType, nullable = true),
      StructField("s4", DoubleType, nullable = true),
      StructField("s5", DoubleType, nullable = true),
      StructField("s6", DoubleType, nullable = true),
      StructField("s7", DoubleType, nullable = true),
      StructField("s8", DoubleType, nullable = true),
      StructField("s9", DoubleType, nullable = true),
      StructField("s10", DoubleType, nullable = true),
      StructField("s11", DoubleType, nullable = true),
      StructField("s12", DoubleType, nullable = true),
      StructField("s13", DoubleType, nullable = true),
      StructField("s14", DoubleType, nullable = true),
      StructField("s15", DoubleType, nullable = true),
      StructField("s16", DoubleType, nullable = true),
      StructField("s17", DoubleType, nullable = true),
      StructField("s18", DoubleType, nullable = true),
      StructField("s19", DoubleType, nullable = true),
      StructField("s20", DoubleType, nullable = true),
      StructField("s21", DoubleType, nullable = true)))
  }

  /*
    This function defines the schema of the truth associated with the test data.
   */
  def getSchemaTruth: StructType = {
    StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("RUL_at_maxcycle", IntegerType, nullable = false)))
  }

  /*
    Standardize a value as std<x> as s - a/sd (signal - average signal / standard deviation)
   */
  def stdizedOperationmode(spark: SparkSession, withrul: DataFrame): DataFrame = {
    val AZ: Column  = lit(0.00000001)

    def opMode(id: Int): Column = {
      (column("s"+id) - coalesce(column("a"+id) / column("sd"+id), column("a"+id) / lit(AZ))).as("std"+id)
    }

    val columns: IndexedSeq[Column] = 1 to 21 map(id => opMode(id))
    val selectAll: Array[Column] = (for (i <- withrul.columns) yield withrul(i)) ++ columns
    withrul.select(selectAll.toIndexedSeq: _*)
  }

  /*
    Calculate the standard deviation and mean for each column over a sliding window.
   */
  def calculateMeanSdev(spark: SparkSession, withrul: DataFrame): DataFrame = {
    val windowRange = 5
    val w = Window.partitionBy("id").orderBy("cycle").rowsBetween(-windowRange + 1, 0)

    def meanCol(id: Int): Column = {
      val col: Column = column("s"+id)
      mean(col).over(w).as("a"+id)
    }

    def stddevCol(id: Int): Column = {
      val col: Column = column("s"+id)
      sqrt(sum(pow(col - mean(col).over(w), 2)).over(w) / windowRange).as("sd"+id)
    }

    val meancols: IndexedSeq[Column] = 1 to 21 map(id => meanCol(id))
    val stddevcols: IndexedSeq[Column] = 1 to 21 map(id => stddevCol(id))

    val selectAll: Array[Column] = (for (i <- withrul.columns) yield withrul(i)) ++ meancols ++ stddevcols
    withrul.select(selectAll.toIndexedSeq: _*)
  }

  def addOPmode(spark: SparkSession, df: DataFrame, operationModePredictions: DataFrame): DataFrame = {
    import spark.implicits._
    val withOPMode = df.as("a").join(
      operationModePredictions.as("b"),
      $"a.id" === $"b.id" and $"a.cycle" === $"b.cycle"
    )
    withOPMode.select($"a.*", $"b.operationmode")
  }

  def addRULtrain(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val maxCyclePerId = df.groupBy($"id").agg(max($"cycle").alias("maxcycle"))
    addRUL(spark, df, maxCyclePerId)
  }

  def addRULtest(spark: SparkSession, df: DataFrame, truth_at_maxcycle: DataFrame): DataFrame = {
    import spark.implicits._

    val ltc_df = df.groupBy($"id").agg(max($"cycle").alias("last_test_cycle"))

    val maxCyclePerId = ltc_df.join(truth_at_maxcycle, "id")
      .withColumn("maxcycle", truth_at_maxcycle("RUL_at_maxcycle") + ltc_df("last_test_cycle"))

    addRUL(spark, df, maxCyclePerId)
  }

  def addRUL(spark: SparkSession, df: DataFrame, maxcycle: DataFrame): DataFrame = {
    val withrul = df.join(maxcycle, "id")
      .withColumn("rul", maxcycle("maxcycle") - df("cycle"))

    withrul.drop("maxcycle")
           .drop("RUL_at_maxcycle")
           .drop("last_test_cycle")
  }

  /*
     add label 2 (1 if under w1, 2 if under w0, zero otherwize)
     */
  def addLabels(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val w1: Int = 30
    val w0: Int = 15

    df.withColumn("label1", when($"rul" <= w1, 1).otherwise(0))
      .withColumn("label2", when($"rul" <= w0, 2).otherwise(when($"rul" <= w1, 1).otherwise(0)))
  }

  /*
     perform K Means clustering based on the operational settings
   */
  def kMeansForOperationalModes(spark: SparkSession, df: DataFrame): (KMeansModel, DataFrame) = {
    import spark.implicits._
    val clusterDF = df.select($"id", $"cycle", $"setting1", $"setting2", $"setting3")

    val assembler = new VectorAssembler()
      .setInputCols(Array("setting1", "setting2", "setting3"))
      .setOutputCol("features")

    val clusterwfeatDF = assembler.transform(clusterDF)

    val kmeans = new KMeans()
      .setK(6)
      .setFeaturesCol("features")
      .setPredictionCol("operationmode")
    val model = kmeans.fit(clusterwfeatDF)

    val operationModePredictions = model.transform(clusterwfeatDF)

    (model, operationModePredictions)
  }

  def readDataFile(spark: SparkSession, input: String): DataFrame = {
    spark.read
      .option("header", "false")
      .option("delimiter", " ")
      .option("mode", "PERMISSIVE")
      .schema(getSchema)
      .csv(input)
  }

  def readRULFile(spark: SparkSession, input: String): DataFrame = {
    spark.read
      .option("header", "false")
      .option("delimiter", " ")
      .option("mode", "PERMISSIVE")
      .schema(getSchemaTruth)
      .csv(input)
  }

  def scaleFeatures(spark: SparkSession, df: DataFrame): DataFrame = {

    val columns = df.columns.diff(Seq("id", "maxcycle", "rul", "label1", "label2"))

    println(s"assembler these columns to  features vector ${columns.toList}")

    val assembler = new VectorAssembler()
      .setInputCols(columns.toArray)
      .setOutputCol("features")
    val withFeatures = assembler.transform(df)

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scaledDF = scaler.fit(withFeatures).transform(withFeatures)

    scaledDF.drop("features").drop("maxcycle")
  }
}


object PrepareTrainData extends PrepareData {
  case class Params(input: String = null, output: String = null)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Prepare train data for sds") {
      head("PrepareData")
      arg[String]("<input_tsv>")
        .required()
        .text("hdfs input paths tsv dataset ")
        .action((x, c) => c.copy(input = x.trim))
      arg[String]("<output_parquet>")
        .required()
        .text("hdfs output paths parquet output ")
        .action((x, c) => c.copy(output = x.trim))
      note(
        """
          |For example, the following command runs this app on a  dataset:
          |
          | spark-submit --class com.combient.sparkjob.tedsds.PrepareTrainData \
          |  jarfile.jar \
          |  "/share/tedsds/input/test_FD001.txt" \
          |  "/share/tedsds/scaleddftest_FD001/"
        """.stripMargin)
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case None         => System.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession.builder()
      .appName("PrepareTrainData")
      .enableHiveSupport()
      .getOrCreate()

    val df = readDataFile(spark, params.input)

    df.cache()

    val (_, operationModePredictions) = kMeansForOperationalModes(spark, df)
    val withOPMode = addOPmode(spark, df, operationModePredictions)
    val withrul   = addRULtrain(spark, withOPMode)
    val withlabels = addLabels(spark, withrul)
    val withMeans = calculateMeanSdev(spark, withlabels)
    val scaledDF  = scaleFeatures(spark, withMeans)

    scaledDF.write.mode(SaveMode.Overwrite).parquet(params.output)

    scaledDF.write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(params.output.concat(".csv"))

    withMeans.write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(params.output.concat("_unscaled.csv"))

    spark.stop()
  }
}


object PrepareTestData extends PrepareData {

  case class Params(input: String = null, truth: String = null, output: String = null, disablecsv: Boolean = false)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Prepare test data for sds") {
      head("PrepareTestData")
      arg[String]("<input_data_tsv>")
        .required()
        .text("hdfs input paths tsv dataset ")
        .action((x, c) => c.copy(input = x.trim))
      arg[String]("<truth_tsv>")
        .required()
        .text("hdfs input paths tsv dataset ")
        .action((x, c) => c.copy(truth = x.trim))
      arg[String]("<output_parquet>")
        .required()
        .text("hdfs output paths parquet output ")
        .action((x, c) => c.copy(output = x.trim))
      opt[Unit]("disablecsv").action { (_, c) =>
        c.copy(disablecsv = true)
      }.text("disablecsv is a flag if false to not write csv")
      note(
        """
          |For example, the following command runs this app on a  dataset:
          |
          | spark-submit --class com.combient.sparkjob.tedsds.PrepareTrainData \
          |  jarfile.jar \
          |  "/share/tedsds/input/test_FD001.txt" \
          |  "/share/tedsds/input/RUL_FD001.txt" \
          |  "/share/tedsds/scaleddftest_FD001/"
        """.stripMargin)
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case None         => System.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession.builder()
      .appName("PrepareTestData")
      .enableHiveSupport()
      .getOrCreate()

    val df    = readDataFile(spark, params.input)
    val truth = readRULFile(spark, params.truth)

    df.cache()
    truth.cache()

    val (_, operationModePredictions) = kMeansForOperationalModes(spark, df)
    val withOPMode = addOPmode(spark, df, operationModePredictions)
    val withrul    = addRULtest(spark, withOPMode, truth)
    val withlabels = addLabels(spark, withrul)
    val withMeans  = calculateMeanSdev(spark, withlabels)
    val scaledDF   = scaleFeatures(spark, withMeans)

    scaledDF.write.mode(SaveMode.Overwrite).parquet(params.output)

    if (params.disablecsv) {
      scaledDF.write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(params.output.concat(".csv"))
      withMeans.write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(params.output.concat("_unscaled.csv"))
    }
    spark.stop()
  }
}
