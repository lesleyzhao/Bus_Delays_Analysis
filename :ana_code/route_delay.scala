// Question 2:
// Does the length of the delay correlate with specific routes (Route_Number)? 
// Delve into the correlation between 
// 1) delay & route
// 2) which times of the day each route experiences the longest delays.
// 3) if certain boroughs have more impact on delays.
// 4) how often delays occur on each route.


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}

val spark = SparkSession.builder.appName("Route Delay Correlation Analysis").getOrCreate()
import spark.implicits._


// Schema definition for the CSV file
val schema = new StructType()
  .add("School_Year", StringType, true)
  .add("Route_Number", StringType, true)
  .add("Reason", StringType, true)
  .add("Occurred_On", TimestampType, true)
  .add("Boro", StringType, true)
  .add("Bus_Company_Name", StringType, true)
  .add("How_Long_Delayed", StringType, true)
  .add("Number_Of_Students_On_The_Bus", DoubleType, true)
  .add("How_Long_Delayed_Clean", DoubleType, true)
  .add("Occurred_On_Formatted", TimestampType, true)
  .add("Bus_Company_Name_Normalized", StringType, true)
  .add("BoroIndex", DoubleType, true)


// Put whatever filename we have in HDFS Bus_Cleaned directory here.
// IMPORTANT: Notice the exact filename may change if you reexecute the previous code
val df = spark.read
  .schema(schema)
  .option("header", "true")
  .csv("Bus_Cleaned/part-00000-e1e78d81-df57-4af1-a07d-0ed189bb09b9-c000.csv")

// Calculate the average delay for each route
val averageDelayPerRoute = df
  .groupBy($"Route_Number")
  .agg(avg($"How_Long_Delayed_Clean").alias("Average_Delay"))
  .orderBy($"Average_Delay".desc)

averageDelayPerRoute.show()

// save the result to a CSV file in HDFS
averageDelayPerRoute.coalesce(1).write.option("header", "true").csv("route_delay_correlation/averageDelayPerRoute")


// Identify which times of the day each route experiences the longest delays. 
val peakDelaysPerRoute = df.withColumn("HourOfDay", hour($"Occurred_On_Formatted"))
  .groupBy($"Route_Number", $"HourOfDay")
  .agg(avg($"How_Long_Delayed_Clean").alias("Average_Delay"))
  .orderBy($"Route_Number", $"Average_Delay".desc)

peakDelaysPerRoute.show()
peakDelaysPerRoute.coalesce(1).write.option("header", "true").csv("route_delay_correlation/peakDelaysPerRoute")


// Analyze if certain boroughs have more impact on delays.
val delaysAcrossBoro = df
  .groupBy($"Route_Number", $"Boro")
  .agg(avg($"How_Long_Delayed_Clean").alias("Average_Delay"))
  .orderBy($"Boro", $"Average_Delay".desc)

delaysAcrossBoro.show()
delaysAcrossBoro.coalesce(1).write.option("header", "true").csv("route_delay_correlation/delaysAcrossBoro")


// Determine how often delays occur on each route. 
val frequencyOfDelaysPerRoute = df
  .groupBy($"Route_Number")
  .count()
  .orderBy($"count".desc)
frequencyOfDelaysPerRoute.show()
frequencyOfDelaysPerRoute.coalesce(1).write.option("header", "true").csv("route_delay_correlation/frequencyOfDelaysPerRoute")


spark.stop()

