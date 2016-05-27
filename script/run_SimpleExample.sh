#!/bin/sh

#Change to top directory
cd ..

#Compile and create the .jar file
sbt assembly

#Make sure files are not there
hadoop fs -rm -r -f SimpleExample.*

#Submit the job
spark-submit --class com.combient.sparkjob.SimpleExample --master yarn ./target/scala-2.10/tedsds-assembly-1.0.jar

#Get files from HDFS
hadoop fs -get SimpleExample.*
