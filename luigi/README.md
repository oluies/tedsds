
Commandline: 
 ./start_luigi.sh  --module spark_tedsds TEDSDSMulticlassMetricsFortedsds

spark-submit --master yarn --class com.combient.sparkjob.tedsds.MulticlassMetricsFortedsds --driver-cores 5  /home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar  "/share/tedsds/scaleddftest_*" "/share/tedsds/savedmodelallrand"
