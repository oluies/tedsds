#!/bin/sh
which python
. /etc/spark/conf/spark-env.sh
export PYSPARK_SUBMIT_ARGS="--packages com.databricks:spark-csv_2.10:1.4.0 --master yarn-client pyspark-shell"

# Spark master url. eg. spark://master_addr:7077. Leave empty if you want to use local mode    
export MASTER=yarn-client        
export SPARK_YARN_JAR=hdfs://hdpvm:8020/hdp/apps/2.4.0.0-169/spark/spark-hdp-assembly.jar
export JAVA_HOME=/usr/jdk64/jdk1.8.0_40
# Additional jvm options. for example, export ZEPPELIN_JAVA_OPTS="-Dspark.executor.memory=8g -Dspark.cores.max=16"
export JAVA_OPTS="-Dhdp.version=2.4.0.0-169 -Dspark.executor.memory=1024m -Dspark.executor.instances=2 -Dspark.yarn.queue=default"
##
# (required) When it is defined, load it instead of Zeppelin embedded Spark libraries
export SPARK_HOME=/usr/hdp/current/spark-client/

export PYTHONPATH="/usr/local/lib/python2.7/dist-packages:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.9-src.zip:.:"
echo " Pythonpath "$PYTHONPATH
export SPARK_YARN_USER_ENV="PYTHONPATH=${PYTHONPATH}"

echo $SPARK_HOME
/usr/local/bin/luigid  --background  $*

