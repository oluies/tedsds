#!/bin/bash

##Usage : 
#
# sparkcsv2csv.se file_written_by_sparkcsv.csv


## This script takes a .csv as written by com.databricks.spark.csv **** option("header", "true") *****
## And transform it into a standart .csv file to be opened in Excel


FILE=`echo $1 | sed "s/\/$//" `

mv $FILE ${FILE}_tmp
head -n 1 ${FILE}_tmp/part-00000  > $FILE
for i in `ls ${FILE}_tmp/part*` ; do cat $i | sed -e "1d" ; done >> $FILE

rm -rf ${FILE}_tmp
