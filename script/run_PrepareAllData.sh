#!/bin/bash



#Simple attempt to figure out the relative path to the data
if [[ $0 == "./script/run_PrepareAllData.sh" ]]
then
	TARGETDIR=./target
else
if [[ $0 == "./run_PrepareAllData.sh" ]]
then
	TARGETDIR=../target
else
	echo "Please run this script from within the script/ directory"
	exit
fi
fi

### Location of the project folder in HDFS
HDFSroot=/share/tedsds


#Define the commands for submitting jobs
SUBMIT_COMMAND_TRAIN="spark-submit --class com.combient.sparkjob.tedsds.PrepareTrainData --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
SUBMIT_COMMAND_TEST="spark-submit --class com.combient.sparkjob.tedsds.PrepareTestData --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"

#Clean HDFS from previous files
hadoop fs -rm -r -f $HDFSroot/scaleddftest*

#Run the data preparation for the 4 train and test sets (this takes a while)
$SUBMIT_COMMAND_TEST $HDFSroot/input/test_FD001.txt $HDFSroot/input/RUL_FD001.txt $HDFSroot/scaleddftest_FD001
$SUBMIT_COMMAND_TEST  $HDFSroot/input/test_FD002.txt $HDFSroot/input/RUL_FD002.txt $HDFSroot/scaleddftest_FD002
$SUBMIT_COMMAND_TEST  $HDFSroot/input/test_FD003.txt $HDFSroot/input/RUL_FD003.txt $HDFSroot/scaleddftest_FD003
$SUBMIT_COMMAND_TEST  $HDFSroot/input/test_FD004.txt $HDFSroot/input/RUL_FD004.txt $HDFSroot/scaleddftest_FD004

$SUBMIT_COMMAND_TRAIN $HDFSroot/input/train_FD001.txt $HDFSroot/scaleddftrain_FD001
$SUBMIT_COMMAND_TRAIN $HDFSroot/input/train_FD002.txt $HDFSroot/scaleddftrain_FD002
$SUBMIT_COMMAND_TRAIN $HDFSroot/input/train_FD003.txt $HDFSroot/scaleddftrain_FD003
$SUBMIT_COMMAND_TRAIN $HDFSroot/input/train_FD004.txt $HDFSroot/scaleddftrain_FD004
