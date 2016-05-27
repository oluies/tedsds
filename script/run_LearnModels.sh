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

### Location of the project folder in HDFS
HDFSroot=/share/tedsds

### Choose were to log output
OUTPUTLOG=./Learn_models.log
ERRORLOG=/dev/null


### The next block of code trains a logistic classifier and analyze its performance on
### on a  validation set created by randomly subsampling the training data

#Define command, clean HDFS and learn a Logistic Classifier on the 4 training sets
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunLogisticRegressionWithValidationData --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f $HDFSroot/example_model*
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD001 $HDFSroot/example_model 2>> $ERRORLOG | tee -a $OUTPUTLOG





#Learn models for each of the two labels
for LABEL in 1 2
do

### Below we train all the models isung all the data for training (= without validation set)
#Define command, clean HDFS and learn a Logistic Classifier on the 4 training sets
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunLogisticRegressionWithLBFGS --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f $HDFSroot/lr_model*_label$LABEL
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD001 $HDFSroot/lr_model_FD001_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD002 $HDFSroot/lr_model_FD002_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD003 $HDFSroot/lr_model_FD003_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD004 $HDFSroot/lr_model_FD004_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG

#The same but with a RandomForest Classifier
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunRandomForest --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f $HDFSroot/rf_model*_label$LABEL
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD001 $HDFSroot/rf_model_FD001_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD002 $HDFSroot/rf_model_FD002_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD003 $HDFSroot/rf_model_FD003_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD004 $HDFSroot/rf_model_FD004_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG

#The same but with a more complex RandomForest Classifier (more  and deeper trees)
SUBMIT_COMMAND="spark-submit --class com.combient.sparkjob.tedsds.RunRandomForest2 --master yarn $TARGETDIR/scala-2.10/tedsds-assembly-1.0.jar"
hadoop fs -rm -r -f $HDFSroot/rf2_model*_label$LABEL
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD001 $HDFSroot/rf2_model_FD001_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD002 $HDFSroot/rf2_model_FD002_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD003 $HDFSroot/rf2_model_FD003_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG
$SUBMIT_COMMAND  $HDFSroot/scaleddftrain_FD004 $HDFSroot/rf2_model_FD004_label$LABEL --label $LABEL 2>> $ERRORLOG | tee -a $OUTPUTLOG

done
