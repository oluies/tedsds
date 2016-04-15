#!/bin/sh

#This script uploads the data from the ../data folder to HDFS

#gunzip --keep ../data/*.gz
for i in `ls ../data/*.gz` 
do 
	j=`echo $i | sed "s/\.gz//" ` 
	gunzip -c $i > $j 
done


#hadoop fs -rm -r -f /share/tedsds
hadoop fs -mkdir -p /share/tedsds/input/
hadoop fs -put ../data/test_FD001.txt /share/tedsds/input/
hadoop fs -put ../data/test_FD002.txt /share/tedsds/input/
hadoop fs -put ../data/test_FD003.txt /share/tedsds/input/
hadoop fs -put ../data/test_FD004.txt /share/tedsds/input/
hadoop fs -put ../data/train_FD001.txt /share/tedsds/input/
hadoop fs -put ../data/train_FD002.txt /share/tedsds/input/
hadoop fs -put ../data/train_FD003.txt /share/tedsds/input/
hadoop fs -put ../data/train_FD004.txt /share/tedsds/input/
hadoop fs -put  ../data/RUL_FD001.txt  /share/tedsds/input/
hadoop fs -put  ../data/RUL_FD002.txt  /share/tedsds/input/
hadoop fs -put  ../data/RUL_FD003.txt  /share/tedsds/input/
hadoop fs -put  ../data/RUL_FD004.txt  /share/tedsds/input/
hadoop fs -ls /share/tedsds/input/

rm -rf ../data/*.txt
