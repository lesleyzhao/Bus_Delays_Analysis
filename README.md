# Bus_Delays_Analysis

This is the README file for executing and exanimating the code for Bus Delay Analysis.

Directories:

* /data_ingest: contains code to do data ingestion.
* /etl_code: contains code to clean the data, remove special chars, obtain integers from strings.
* /profiling_code: contains code to look at the features of each columns, like min, max, average, range, and so on.
* /ana_code: contains code for analytics. 

  Execution order: trend.scala -> trend2.scala -> route_delay.scala -> route_delay2.scala -> prediction.scala



Procedures:
Step 1 Data Ingestion: 
* First please upload /data_ingest/ingestion.scala to your Dataproc local by using UPLOAD FILE functionality on the white bar below. Then you can enter the command ls to check whether the upload is successful.
 
* Then, run the following to execute ingestion.scala by reading dataset from a file called “Bus_Breakdown_and_Delays_20231105.csv” on HDFS
 spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i ingestion.scala
or 
spark-shell --deploy-mode client -i ingestion.scala



Step 2 Cleaning & ETL 
* Please also upload Clean1.scala in /etl_code as we did in Step 1.
* Run: 
spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i Clean1.scala
or 
spark-shell --deploy-mode client -i Clean1.scala
 
Notice: this line may report error because Bus_Cleaned directory is already created on my HDFS. You can change the directory name to “Bus_Cleaned2” or delete the original “Bus_Cleaned” directory. You may also see this error when you rerun other scala code, just change another directory name inside .csv() to make it work.
•	You should see results like this. Previously I saved results on HDFS Bus_Cleaned directory. You may change the directory name if you follow the notice above.
 

Step 3 Profiling
* Please also upload countRecs.scala in /profiling_code as we did in Step 1.
* Run:
spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i countRecs.scala
or 
spark-shell --deploy-mode client -i countRecs.scala
	Notice: the exact filename and directory name below may change 
            , if you rerun the previous ETL code.
 
Put the path of your resulting csv that is generated in Step 2 here inside the parenthesis.

 
Around line 132, If you encounter the previous “directory already existed” error, please change to another directory name (like “Bus_Profiling_Stats2”) to save the results.

 * You should see results like this. Previously I saved all results on HDFS “Bus_Profiling_Stats” directory. You may change the directory name if you follow the notice above.
 


Step 4 Analytics
* 	Please also upload all scala files in /ana_code as we did in Step 1.
* 	Execute scala files based on this order: 
trend.scala -> trend2.scala -> route_delay.scala -> route_delay2.scala -> prediction.scala
To do this, run the following commands:
spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i trend.scala
or 
spark-shell --deploy-mode client -i trend.scala

spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i trend2.scala
or 
spark-shell --deploy-mode client -i trend2.scala

spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i route_delay.scala
or 
spark-shell --deploy-mode client -i route_delay.scala

spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i route_delay2.scala
or 
spark-shell --deploy-mode client -i route_delay2.scala

spark-shell --deploy-mode client --driver-memory 1g --executor-memory 2g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.memoryOverhead=512m -i prediction.scala
or 
spark-shell --deploy-mode client -i prediction.scala
* 	I saved all results on HDFS ‘monthly_trend’ ‘hourly_trend’ ‘daily_trend’ ‘route_delay_correlation’ directories. You may change the directory name if you follow the notice below.

             Notice: These two errors may happen multiple times 
             ,if you rerun the previous scala code. 
             But you can solve them using the following steps.
* You may also encounter the previous import csv error: path doesn’t exist because you change the previous saving path when executing previous files. Just replace path with the correct path that you saved the prior result.
 
* You may also encounter the previous saving error, saying that the path is already created. In this case, we just need to save it to another new directory to make it work. Like below replacing ‘hourly_trend’ with ‘hourly_trend2’. 
 

