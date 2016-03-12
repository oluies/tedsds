Luigi is a Python module that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization etc. It also comes with Hadoop support built in.

See [https://github.com/spotify/luigi](https://github.com/spotify/luigi)

## Commandlines for these examples : 
1. ```sh
./start_luigi.sh  --module spark_tedsds TEDSDSMulticlassMetricsFortedsds```
2. ```sh
./start_luigi.sh --workers=3 --module spark_tedsds PrepareAllData```
3. ```sh
spark-submit --master yarn --class com.combient.sparkjob.tedsds.MulticlassMetricsFortedsds --driver-cores 5  /home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar  "/share/tedsds/scaleddftest_*" "/share/tedsds/savedmodelallrand"```
