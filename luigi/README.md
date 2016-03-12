

Commandline: 
 ./start_luigi.sh  --module spark_tedsds TEDSDSMulticlassMetricsFortedsds
<<<<<<< HEAD
 ./start_luigi.sh  --workers=3 --module spark_tedsds PrepareAllData
=======

spark-submit --master yarn --class com.combient.sparkjob.tedsds.MulticlassMetricsFortedsds --driver-cores 5  /home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar  "/share/tedsds/scaleddftest_*" "/share/tedsds/savedmodelallrand"
>>>>>>> fcb6ed89fdf2ec35466cb8d2ca073856d222b789
