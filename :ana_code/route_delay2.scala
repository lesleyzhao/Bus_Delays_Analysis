// Continue on route_delay.scala for Question 2
// 1).Explore which Boros have the most delays in 'delayAcrossBoro' dataset
// 2).Obtain which hours are the most common delayed hours based on Route_Number
// 3).Determine which hours are busy hours for buses.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}


val spark = SparkSession.builder.appName("Borough Delay Analysis").getOrCreate()
import spark.implicits._

val schema = new StructType()
  .add("Route_Number", StringType, true)
  .add("Boro", StringType, true)
  .add("Average_Delay", DoubleType, true)

val df = spark.read
  .schema(schema)
  .option("header", "true")
  .csv("route_delay_correlation/delaysAcrossBoro/part-00000-5028fc28-a2bc-437e-8121-3ba9e221e8c9-c000.csv")

// 1). Group by the Boro and calculate the average delay
val boroDelayOrder = df
  .groupBy($"Boro")
  .agg(avg($"Average_Delay").alias("Avg_Delay"))
  .orderBy($"Avg_Delay".desc)

boroDelayOrder.show()

// 2).Obtain the most common delayed hour for each Route_Number

val schema2 = new StructType()
  .add("Route_Number", StringType, true)
  .add("HourOfDay", IntegerType, true)
  .add("Average_Delay", DoubleType, true)
val dfhours = spark.read
  .schema(schema2)
  .option("header", "true")
  .csv("route_delay_correlation/peakDelaysPerRoute/part-00000-ba2e1688-1fe8-4f95-8815-fea43b1b692c-c000.csv")

val mostCommonDelayPerRoute = dfhours
  .groupBy($"Route_Number", $"HourOfDay")
  .agg(count($"HourOfDay").alias("Count"))
  .withColumnRenamed("HourOfDay", "MostCommonDelayedHour")
  .orderBy($"Route_Number", $"Count".desc)


mostCommonDelayPerRoute.show()

// 3).Determine the overall busiest hours for buses across all routes
val busiestHours = dfhours
  .groupBy($"HourOfDay")
  .agg(count($"HourOfDay").alias("TotalDelays"))
  .orderBy($"TotalDelays".desc)

busiestHours.show()


boroDelayOrder.coalesce(1).write.option("header", "true").csv("route_delay_correlation/boroDelayOrder")
mostCommonDelayPerRoute.coalesce(1).write.option("header", "true").csv("route_delay_correlation/mostCommonDelayPerRoute")
busiestHours.coalesce(1).write.option("header", "true").csv("route_delay_correlation/busiestHours")

spark.stop()
