#!/bin/bash

#Simple attempt to figure out the relative path to the data
if [[ $0 == "./script/run_toy.sh" ]]
then
	:
else
if [[ $0 == "./run_toy.sh" ]]
then
	cd ..
else
	echo "Please run this script from within the script/ directory"
	exit
fi
fi
TARGETDIR=./target
### Location of the project folder in HDFS
HDFSroot=/share/tedsds

#Compile and assemble the code
sbt assembly

#Make sure the files are on HDFS
./script/hadoopfs.sh

#This defines the submit commands
SUBMIT_COMMAND_TRAIN="spark-submit --class com.combient.sparkjob.tedsds.PrepareTrainData --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
SUBMIT_COMMAND_TEST="spark-submit --class com.combient.sparkjob.tedsds.PrepareTestData --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"


#Execute the data preparation on the toy data
$SUBMIT_COMMAND_TRAIN $HDFSroot/input/train_toy.txt $HDFSroot/toy_train
$SUBMIT_COMMAND_TEST $HDFSroot/input/test_toy.txt $HDFSroot/input/RUL_toy.txt  $HDFSroot/toy_test

#Get the data out of HDFS
mkdir -p tmp_data
cd tmp_data
rm -rf ./*
hadoop fs -get $HDFSroot/toy* ./

#Convert the csv files from Spark-csv to normal csv files
../script/sparkcsv2csv.sh ./toy_train_unscaled.csv/
../script/sparkcsv2csv.sh ./toy_train.csv/
../script/sparkcsv2csv.sh ./toy_test.csv/
../script/sparkcsv2csv.sh ./toy_test_unscaled.csv/
