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


object PrepareData2 {

  case class Params(input: String = null, output: String = null)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Prepare data for sds") {
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
          | bin/spark-submit --class com.combient.sparkjob.tedsds.PrepareData \
          |  jarfile.jar \
          |  "/user/zeppelin/pdm001/tmp.83h1YRpXM2/train_FD001.txt" \
          |  "/share/tedsds/scaledd"
        """.stripMargin)
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case None         => System.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession.builder()
      .appName("PrepareData2")
      .enableHiveSupport()
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "false")
      .option("delimiter", " ")
      .option("mode", "PERMISSIVE")
      .schema(getSchema)
      .csv(params.input)

    df.persist()
    df.cache()

    val (_, operationModePredictions) = kMeansForOperationalModes(spark, df)
    val withOPMode = addOPmode(spark, df, operationModePredictions)
    val withrul    = addLabels(spark, withOPMode)
    val withMeans  = calculateMeanSdev(spark, withrul)

    val columns = withMeans.columns.diff(Seq("id", "maxcykle", "rul", "label1", "label2"))
    println(s"assembler these columns to  features vector ${columns.toList}")

    val assembler = new VectorAssembler()
      .setInputCols(columns.toArray)
      .setOutputCol("features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val withFeatures = assembler.transform(withMeans)
    val scaledDF    = scaler.fit(withFeatures).transform(withFeatures)

    scaledDF.drop("features")
    scaledDF.drop("maxcykle")

    scaledDF.write.mode(SaveMode.Overwrite).parquet(params.output)

    spark.stop()
  }

  /*
    Return the schema
   */
  def getSchema: StructType = {
    StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("cykle", IntegerType, nullable = false),
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

  def stdizedOperationmode(spark: SparkSession, withrul: DataFrame): DataFrame = {
    val AZ: Column = lit(0.00000001)

    def opMode(id: Int): Column = {
      (column("s"+id) - coalesce(column("a"+id) / column("sd"+id), column("a"+id) / lit(AZ))).as("std"+id)
    }

    val columns: IndexedSeq[Column] = 1 to 21 map(id => opMode(id))
    val selectAll: Array[Column] = (for (i <- withrul.columns) yield withrul(i)) ++ columns
    withrul.select(selectAll.toIndexedSeq: _*)
  }

  def calculateMeanSdev(spark: SparkSession, withrul: DataFrame): DataFrame = {
    val windowRange = 5
    val w = Window.partitionBy("operationmode").orderBy("id", "cykle").rowsBetween(0, windowRange)

    def meanCol(id: Int): Column = {
      val col: Column = column("s"+id)
      mean(col).over(w).as("a"+id)
    }

    def stddevCol(id: Int): Column = {
      val col: Column = column("s"+id)
      sqrt(sum(pow(col - mean(col).over(w), 2)).over(w) / windowRange).as("sd"+id)
    }

    val meancols: IndexedSeq[Column]   = 1 to 21 map(id => meanCol(id))
    val stddevcols: IndexedSeq[Column] = 1 to 21 map(id => stddevCol(id))

    val selectAll: Array[Column] = (for (i <- withrul.columns) yield withrul(i)) ++ meancols ++ stddevcols
    withrul.select(selectAll.toIndexedSeq: _*)
  }

  def addOPmode(spark: SparkSession, df: DataFrame, operationModePredictions: DataFrame): DataFrame = {
    import spark.implicits._
    val withOPMode = df.as("a").join(
      operationModePredictions.as("b"),
      $"a.id" === $"b.id" and $"a.cykle" === $"b.cykle"
    )
    withOPMode.select($"a.*", $"b.operationmode")
  }

  /*
     add label 2 (1 if under w1, 2 if under w0, zero otherwize)
   */
  def addLabels(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val maxCyclePerId = df.groupBy($"id").agg(max($"cykle").alias("maxcykle"))

    val w1: Int = 30
    val w0: Int = 15

    df.join(maxCyclePerId, "id")
      .withColumn("rul", maxCyclePerId("maxcykle") - df("cykle"))
      .withColumn("label1", when($"rul" <= w1, 1).otherwise(0))
      .withColumn("label2", when($"rul" <= w0, 2).otherwise(when($"rul" <= w1, 1).otherwise(0)))
  }

  /*
     perform K Means clustering based on the operational settings
   */
  def kMeansForOperationalModes(spark: SparkSession, df: DataFrame): (KMeansModel, DataFrame) = {
    import spark.implicits._
    val clusterDF = df.select($"id", $"cykle", $"setting1", $"setting2", $"setting3")

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
}
