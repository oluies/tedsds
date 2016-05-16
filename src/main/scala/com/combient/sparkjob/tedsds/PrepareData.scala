package com.combient.sparkjob.tedsds

/**
  * Created by olu on 09/03/16.
  */

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.expressions.{WindowSpec, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.spark.ml.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors

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
  def stdizedOperationmode(sqLContext: SQLContext, withrul: DataFrame): DataFrame = {
    // see http://spark.apache.org/docs/latest/sql-programming-guide.html
    import sqLContext.implicits._
    val AZ: Column  = lit(0.00000001)

    def opMode(id:Int): Column = {
      (column("s"+id) - coalesce(column("a"+id) / column("sd"+id), column("a"+id) / lit(AZ))).as("std"+id)
    }

    // add the 21 std<i> columns based on s<i> - (a<id>/sd<id>)
    val columns: IndexedSeq[Column] = 1 to 21 map(id => opMode(id))
    val allColumns = withrul.columns union columns
    val selectAll: Array[Column] = (for (i <- withrul.columns) yield withrul(i)) union columns.toSeq
    val withStd = withrul.select(selectAll :_*)

    withStd
  }

  /*
    Calculate the standard deviation and mean for each column

    Same as :
    val x = withrul.select('*,
      mean($"s1").over(w).as("a1"),
      sqrt( sum(pow($"s1" -  mean($"s1").over(w),2)).over(w) / 5).as("sd1"),

      mean($"s2").over(w).as("a2"),
      sqrt( sum(pow($"s2" -  mean($"s2").over(w),2)).over(w) / 5).as("sd2"),

      mean($"s3").over(w).as("a3"),
      sqrt( sum(pow($"s3" -  mean($"s3").over(w),2)).over(w) / 5).as("sd3"),


   */
  def calculateMeanSdev(sqLContext: SQLContext,withrul: DataFrame): DataFrame = {
    // see http://spark.apache.org/docs/latest/sql-programming-guide.html
    import sqLContext.implicits._

    val windowRange = 5
    // see https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
    //     http://spark.apache.org/docs/latest/sql-programming-guide.html
    // PARTITION BY id  ORDER BY cycle ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING (5)
    // RQ by Nicolas: this should really be changed to rows "-5:0" --- -2:2 is equivalent to do a look ahead in the future

    val w = Window.partitionBy("id").orderBy("cycle").rowsBetween(-windowRange+1,0)

    def meanCol(id:Int): Column = {
      val col: Column = column("s"+id)
      mean(col).over(w).as("a"+id)
    }

    def stddevCol(id:Int): Column = {
      val col: Column = column("s"+id)
      sqrt(sum(pow(col- mean(col).over(w), 2)).over(w) / windowRange).as("sd"+id)
    }

    // add the 21 columns
    val meancols: IndexedSeq[Column] = 1 to 21 map(id => meanCol(id))
    val stddevcols: IndexedSeq[Column] = 1 to 21 map(id => stddevCol(id))

    val selectAll: Array[Column] = (for (i <- withrul.columns) yield withrul(i)) union meancols.toSeq union stddevcols.toSeq

    // :_* - expands the sequence for the vararg constructor in select
    // see The Scala Language Specification 4.6.2 Repeated Parameters"
    val withMeans = withrul.select(selectAll :_*)

    withMeans
  }

  def addOPmode(sqLContext: SQLContext,df: DataFrame, operationModePredictions: DataFrame): DataFrame = {
    import sqLContext.implicits._
    val withOPMode = df.as('a).join(operationModePredictions.as('b), $"a.id" === $"b.id" and $"a.cycle" === $"b.cycle")

    withOPMode.select($"a.*", $"b.operationmode" )
  }

  def addRULtrain(sqLContext: SQLContext,df: DataFrame): DataFrame = {
    import sqLContext.implicits._

    //Finds the last cycle in the data (which corresponds to the failure in the training data)
    val maxCyclePerId = df.groupBy($"id").agg(max($"cycle").alias("maxcycle"))

    addRUL(sqLContext,df,maxCyclePerId)
  }

  def addRULtest(sqLContext: SQLContext,df: DataFrame,truth_at_maxcycle: DataFrame): DataFrame = {
      import sqLContext.implicits._


      val ltc_df = df.groupBy($"id").agg(max($"cycle").alias("last_test_cycle"))

      val maxCyclePerId = ltc_df.join(truth_at_maxcycle,"id")
                 .withColumn("maxcycle",truth_at_maxcycle("RUL_at_maxcycle")+ltc_df("last_test_cycle"))

      addRUL(sqLContext,df,maxCyclePerId)
  }

  def addRUL(sqLContext: SQLContext,df: DataFrame,maxcycle: DataFrame): DataFrame = {
      import sqLContext.implicits._

      //Calculates the RUL and applies the labels
      val withrul = df.join(maxcycle, "id")
        .withColumn("rul", maxcycle("maxcycle") - df("cycle")) // Add RUL as maxcycle-currentcycle per row

      withrul.drop("maxcycle")
             .drop("RUL_at_maxcycle")
             .drop("last_test_cycle")
  }

  /*
     add label 2 (1 if under w1, 2 if under w0, zero otherwize)
     */
  def addLabels(sqLContext: SQLContext,df: DataFrame): DataFrame = {
    import sqLContext.implicits._

    //Define the values for converting the remaining usefull life (RUL) into labels
    val w1: Int = 30
    val w0: Int = 15

    val withlabels = df
        .withColumn("label1", when($"rul" <= w1, 1).otherwise(0)) // add label 1
        .withColumn("label2", when($"rul" <= w0, 2).otherwise(when($"rul" <= w1, 1).otherwise(0)))
    withlabels
  }

  /*
     perform K Means clustering based on the operational settings

   */
  def kMeansForOperationalModes(sqLContext: SQLContext,df: DataFrame): (KMeansModel, DataFrame) = {
    // https://spark.apache.org/docs/latest/ml-clustering.html
    import sqLContext.implicits._
    val clusterDF = df.select($"id",$"cycle",$"setting1", $"setting2", $"setting3")
    //see https://spark.apache.org/docs/latest/ml-features.html
    // columns to feature vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("setting1", "setting2", "setting3"))
      .setOutputCol("features")

    val clusterwfeatDF  = assembler.transform(clusterDF)
    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(6) //  6 operationmodes known apriori from dataset description
      .setFeaturesCol("features")
      .setPredictionCol("operationmode")
    val model = kmeans.fit(clusterwfeatDF)

    val operationModePredictions = model.transform(clusterwfeatDF)

    (model, operationModePredictions)
  }

  def readDataFile(sqlContext: SQLContext, input : String): DataFrame = {
    val customSchema = getSchema

    //Reads the data from disc and create a Spark DataFrame (~a data table)
    val df: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter"," ")
      //.option("treatEmptyValuesAsNulls","true")
      .option("mode","PERMISSIVE")
      .schema(customSchema)
      .load(input)
    df
  }


  def readRULFile(sqlContext: SQLContext, input : String): DataFrame = {

      val truthSchema = getSchemaTruth

      val df: DataFrame = sqlContext.read
                    .format("com.databricks.spark.csv")
                    .option("header", "false")
                    .option("delimiter"," ")
                    //.option("treatEmptyValuesAsNulls","true")
                    .option("mode","PERMISSIVE")
                    .schema(truthSchema)
                    .load(input)
     df
  }

  def scaleFeatures(sqlContext: SQLContext, df : DataFrame): DataFrame = {

        // Filter away columns the features set
        val columns = df.columns.diff(Seq("id","maxcycle","rul","label1", "label2"))

        //These columns had the lowest correlation factor :  "sd11","sd20","sd4","sd12","sd17","sd8","sd15","sd7","sd2","sd3","sd21","setting1","setting2"
        //Alternatively, we could try like this
        //val columns = df.columns.diff(Seq("id","maxcycle","rul","label1", "label2","sd11","sd20","sd4","sd12","sd17","sd8","sd15","sd7","sd2","sd3","sd21","setting1","setting2"))

        println(s"assembler these columns to  features vector ${columns.toList}")

        //see https://spark.apache.org/docs/latest/ml-features.html
        // columns to feature vector
        val assembler = new VectorAssembler()
          .setInputCols(columns.toArray)
          .setOutputCol("features")
        val withFeatures = assembler.transform(df)


        // Define a scaler for rescaling the data so that all features have the same dynamic range.
        // https://spark.apache.org/docs/1.5.2/api/java/org/apache/spark/ml/feature/MinMaxScaler.html
        val scaler = new MinMaxScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")

        // An alternative could be mean/std scaling
        // Or several other options: https://spark.apache.org/docs/1.5.2/api/java/org/apache/spark/ml/Estimator.html
        /*
        val scaler = new StandardScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
        */

        //This line performs the transformation
        val scaledDF =  scaler.fit(withFeatures).transform(withFeatures)

        //Drop temporary features:  featurescolumn, maxcycle
        scaledDF.drop("features")
                .drop("maxcycle")

  }
}


object PrepareTrainData extends PrepareData {
  case class Params(input: String = null,output: String = null)

  def main(args: Array[String]) {
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


    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {

      //Initialiaze Spark: http://spark.apache.org/docs/latest/programming-guide.html#initializing-spark
      val conf = new SparkConf().setAppName("PrepareData")
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)  //Initialiaze Hive: https://hive.apache.org/

      import sqlContext.implicits._ //https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/SQLContext.implicits$.html

      val df = readDataFile(sqlContext,params.input)

      df.cache() // Tell Spark to (try to) keep the data in memory. See: http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence

      //Group the data by operation modes
      val (model: KMeansModel, operationModePredictions: DataFrame) = kMeansForOperationalModes(sqlContext,df)

      // Add operational modes to the dataframe
      val withOPMode: DataFrame = addOPmode(sqlContext,df, operationModePredictions)

      //Calculates the remaining useful life (RUL), creates the labels and add them to the DataFrame
      val withrul: DataFrame = addRULtrain(sqlContext,withOPMode)


      val withlabels: DataFrame = addLabels(sqlContext,withrul)

      //Calculate means and standard over a time window
      val withMeans: DataFrame = calculateMeanSdev(sqlContext,withlabels)

      val scaledDF: DataFrame = scaleFeatures(sqlContext,withMeans)



      //Write the result into parquet format.
      scaledDF.write.mode(SaveMode.Overwrite).parquet(params.output)


      //Optionally save as CSV (for debugging purposes)
      scaledDF.write.mode(SaveMode.Overwrite)
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .save(params.output.concat(".csv"))

      withMeans.write.mode(SaveMode.Overwrite)
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .save(params.output.concat("_unscaled.csv"))


      sc.stop()
    }
}


object PrepareTestData extends PrepareData {

  case class Params(input: String = null,truth: String = null,output: String = null,disablecsv:Boolean = false)

  def main(args: Array[String]) {
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
      opt[Unit]("disablecsv") action { (_, c) =>
        c.copy(disablecsv = true) } text("disablecsv is a flag if false to not write csv")
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


      parser.parse(args, defaultParams).map { params =>
        run(params)
      } getOrElse {
        System.exit(1)
      }
    }

    def run(params: Params) {

      //Initialiaze Spark: http://spark.apache.org/docs/latest/programming-guide.html#initializing-spark
      val conf = new SparkConf().setAppName("PrepareData")
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)  //Initialiaze Hive: https://hive.apache.org/

      import sqlContext.implicits._ //https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/SQLContext.implicits$.html

      //Reads the data from disc and create a Spark DataFrame (~a data table)
      val df = readDataFile(sqlContext,params.input)

      //Read the truth
      val truth = readRULFile(sqlContext,params.truth)


      df.cache() // Tell Spark to keep the data in memory. See: http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence
      truth.cache()

      //Group the data by operation modes
      val (model: KMeansModel, operationModePredictions: DataFrame) = kMeansForOperationalModes(sqlContext,df)

      // Add operational modes to the dataframe
      val withOPMode: DataFrame = addOPmode(sqlContext,df, operationModePredictions)

      //Calculates the remaining useful life (RUL), creates the labels and add them to the DataFrame
      val withrul: DataFrame = addRULtest(sqlContext,withOPMode,truth)

      val withlabels: DataFrame = addLabels(sqlContext,withrul)

      //Calculate means and standard over a time window
      val withMeans: DataFrame = calculateMeanSdev(sqlContext,withlabels)

      //Scale the features
      val scaledDF: DataFrame = scaleFeatures(sqlContext,withMeans)


      //Write the result into parquet format.
      scaledDF.write.mode(SaveMode.Overwrite).parquet(params.output)


      if(params.disablecsv){
        //Optionally save as CSV (for debugging purposes)
        scaledDF.write.mode(SaveMode.Overwrite)
                      .format("com.databricks.spark.csv")
                      .option("header", "true")
                      .save(params.output.concat(".csv"))
        withMeans.write.mode(SaveMode.Overwrite)
                      .format("com.databricks.spark.csv")
                      .option("header", "true")
                      .save(params.output.concat("_unscaled.csv"))
      }
      sc.stop()
    }

  }
