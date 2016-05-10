#!/bin/bash


#This script uploads the data from the data/ folder to HDFS


#Simple attempt to figure out the relative path to the data
if [[ $0 == "./script/hadoopfs.sh" ]]
then
	DATADIR=./data
else
if [[ $0 == "./hadoopfs.sh" ]]
then
	DATADIR=../data
else
	echo "Please run this script from within the script/ directory"
	exit
fi
fi


#Decompress the data
for i in `ls $DATADIR/test_*.gz $DATADIR/train_*.gz`
do
	j=`echo $i | sed "s/\.gz//" `
	gunzip -c $i > $j
done

#Add IDs to the Truth file
for i in `ls $DATADIR/RUL_*.gz `
do
    j=`echo $i | sed "s/\.gz//" `
	gunzip -c  $i | awk '{printf "%d %s\n", NR, $0}' > $j
done

ROOTHDFSFOLDER=tedsds

#Clean up HDFS to avoid "Error - file exist"
hadoop fs -rm -r -f /share/${ROOTHDFSFOLDER}

#Put the data
hadoop fs -mkdir -p /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/test_FD001.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/test_FD002.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/test_FD003.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/test_FD004.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/train_FD001.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/train_FD002.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/train_FD003.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/train_FD004.txt /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/RUL_FD001.txt  /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/RUL_FD002.txt  /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/RUL_FD003.txt  /share/${ROOTHDFSFOLDER}/input/
hadoop fs -put $DATADIR/RUL_FD004.txt  /share/${ROOTHDFSFOLDER}/input/


# The files below are only required for the toy example 'run_toy.sh'
hadoop fs -put $DATADIR/RUL_toy.txt  /share/tedsdsolu/input/
hadoop fs -put $DATADIR/train_toy.txt  /share/tedsdsolu/input/
hadoop fs -put $DATADIR/test_toy.txt  /share/tedsdsolu/input/



#Print the content of the folder in HDFS
hadoop fs -ls /share/tedsdsolu/input/

#Remove uncompressed data
rm -rf $DATADIR/*.txt
