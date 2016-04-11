Luigi is a Python module that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization etc. It also comes with Hadoop support built in.

See [https://github.com/spotify/luigi](https://github.com/spotify/luigi)

## Commandlines for these examples : 
1. ./start_luigi.sh  --module spark_tedsds TEDSDSMulticlassMetricsFortedsds
2. ./start_luigi.sh --workers=3 --module spark_tedsds PrepareAllData
3.  spark-submit --master yarn --class com.combient.sparkjob.tedsds.MulticlassMetricsFortedsds --driver-cores 5  /home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar  "/share/tedsds/scaleddftest_*" "/share/tedsds/savedmodelallrand"
4.  prepare data Model 2 
spark-submit --master yarn --class com.combient.sparkjob.tedsds.PrepareData2 /home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar  "/share/tedsds/input/test_FD001.txt" "/share/tedsds/scaleddftest_FD001_2PD2"
5. LogisticRegressionWithLBFGS
 spark-submit --master yarn --class com.combient.sparkjob.tedsds.RunLogisticRegressionWithLBFGS --driver-cores 5  /home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar  "/share/tedsds/scaleddftest_FD001_2PD2" "/share/tedsds/olulr_001PD2"
