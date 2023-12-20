// Question 1
// Trend Analysis:
// How does bus delay duration (How_Long_Delayed) change over time (Occurred_On)?
// Can we see any trends in delays related to specific times of the day or months of the year?

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}


val spark = SparkSession.builder.appName("Bus Delays Analysis").getOrCreate()

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
  .add("ReasonIndex", DoubleType, true)

// Put whatever filename we have in HDFS Bus_Cleaned directory here.
// IMPORTANT: Notice the exact filename may change if you reexecute the previous ETL code
val df = spark.read
  .schema(schema)
  .option("header", "true")
  .csv("Bus_Cleaned/part-00000-e1e78d81-df57-4af1-a07d-0ed189bb09b9-c000.csv")

// Since 'Occurred_On_Formatted' is already a Timestamp, we can use it directly
// Extract hour, day, month, year from 'Occurred_On_Formatted'
val dfWithTimeData = df
  .withColumn("Hour", hour(col("Occurred_On_Formatted")))
  .withColumn("Day", dayofmonth(col("Occurred_On_Formatted")))
  .withColumn("Month", month(col("Occurred_On_Formatted")))
  .withColumn("Year", year(col("Occurred_On_Formatted")))

// Analyze trends by different time components

// Trend over months
val monthlyTrend = dfWithTimeData.groupBy("Month", "Year")
  .agg(avg("How_Long_Delayed_Clean").alias("Average_Delay"))
  .orderBy("Year", "Month")
monthlyTrend.show()

// Save the entire monthly trend DataFrame to a CSV file in HDFS
monthlyTrend.coalesce(1).write.option("header", "true").csv("monthly_trend")
//mostDelayedMonthEachYear.coalesce(1).write.option("header", "true").csv("monthly_trend/most_delayed_month_yearly")


// Trend over days
val dailyTrend = dfWithTimeData.groupBy("Day", "Month", "Year")
  .agg(avg("How_Long_Delayed_Clean").alias("Average_Delay"))
  .orderBy("Year", "Month", "Day")
dailyTrend.show()


// Save the entire daily trend DataFrame to a CSV file in HDFS
dailyTrend.coalesce(1).write.option("header", "true").csv("daily_trend")
//mostDelayedDayEachMonth.coalesce(1).write.option("header", "true").csv("daily_trend/most_delayed_day_monthly")

// Trend over hours of the day
val hourlyTrend = dfWithTimeData.groupBy("Hour")
  .agg(avg("How_Long_Delayed_Clean").alias("Average_Delay"))
  .orderBy("Hour")
hourlyTrend.show()

// Save the entire hourly trend DataFrame to a CSV file in HDFS
hourlyTrend.coalesce(1).write.option("header", "true").csv("hourly_trend")

spark.stop()
