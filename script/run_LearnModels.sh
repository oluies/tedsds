#!/bin/bash



#Simple attempt to figure out the relative path to the data
if [[ $0 == "./script/run_LearnModels.sh" ]]
then
	TARGETDIR=./target
else
if [[ $0 == "./run_LearnModels.sh" ]]
then
	TARGETDIR=../target
else
	echo "Please run this script from within the script/ directory"
	exit
fi
fi

### The next block of code trains a logistic classifier and analyze its performance on 
### on a  validation set created by randomly subsampling the training data

#Define command, clean HDFS and learn a Logistic Classifier on the 4 training sets
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunLogisticRegressionWithValidationData --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f /share/tedsds/example_model*
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD001 /share/tedsds/example_model


### Below we train all the models isung all the data for training (= without validation set)
#Define command, clean HDFS and learn a Logistic Classifier on the 4 training sets
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunLogisticRegressionWithLBFGS --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f /share/tedsds/lr_model*
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD001 /share/tedsds/lr_model_FD001
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD002 /share/tedsds/lr_model_FD002
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD003 /share/tedsds/lr_model_FD003
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD004 /share/tedsds/lr_model_FD004

#The same but with a RandomForest Classifier
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunRandomForest --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f /share/tedsds/rf_model*
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD001 /share/tedsds/rf_model_FD001
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD002 /share/tedsds/rf_model_FD002
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD003 /share/tedsds/rf_model_FD003
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD004 /share/tedsds/rf_model_FD004

#The same but with a more complex RandomForest Classifier (more  and deeper trees)
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunRandomForest2 --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f /share/tedsds/rf2_model*
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD001 /share/tedsds/rf2_model_FD001
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD002 /share/tedsds/rf2_model_FD002
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD003 /share/tedsds/rf2_model_FD003
$SUBMIT_COMMAND  /share/tedsds/scaleddftrain_FD004 /share/tedsds/rf2_model_FD004
