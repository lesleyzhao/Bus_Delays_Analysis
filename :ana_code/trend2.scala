// Continue on trend.scala for Question 1:
// Question 1
// Trend Analysis:
// How does bus delay duration (How_Long_Delayed) change over time (Occurred_On)?
// Can we see any trends in delays related to specific times of the day or months of the year?


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


  val spark = SparkSession.builder.appName("Bus Delays Analysis - Further Analysis").getOrCreate()
  import spark.implicits._

  // Assuming the data has been loaded and cleaned in the main script and these are the paths for their storage
  val monthlyTrendPath = "monthly_trend/part-00000-d869d052-4afd-4a50-bdd8-759877aa42e3-c000.csv"
  val dailyTrendPath = "daily_trend/part-00000-22c97f89-bec6-44b7-800a-65a3be5c4155-c000.csv"

  // Load the monthly and daily trend DataFrames from HDFS
  val monthlyTrend = spark.read.option("header", "true").csv(monthlyTrendPath)
  val dailyTrend = spark.read.option("header", "true").csv(dailyTrendPath)

  // Cast the average delay to double for aggregation
  val monthlyTrendDouble = monthlyTrend.withColumn("Average_Delay", $"Average_Delay".cast("double"))
  val dailyTrendDouble = dailyTrend.withColumn("Average_Delay", $"Average_Delay".cast("double"))

  // Find the most delayed month for each year
  val mostDelayedMonthEachYear = monthlyTrendDouble
    .groupBy("Year")
    .agg(max("Average_Delay").alias("Max_Average_Delay"))
    .join(monthlyTrendDouble, Seq("Year"))
    .where($"Average_Delay" === $"Max_Average_Delay")
    .select("Year", "Month", "Average_Delay")
    .orderBy($"Year", $"Average_Delay".desc)

  mostDelayedMonthEachYear.show()

  // Save the most delayed month for each year to a CSV file in HDFS
  mostDelayedMonthEachYear.coalesce(1).write.option("header", "true").csv("monthly_trend/most_delayed_month_yearly")

  // Find the most delayed day for each month and year
  val mostDelayedDayEachMonth = dailyTrendDouble
    .groupBy("Month", "Year")
    .agg(max("Average_Delay").alias("Max_Average_Delay"))
    .join(dailyTrendDouble, Seq("Month", "Year"))
    .where($"Average_Delay" === $"Max_Average_Delay")
    .select("Day", "Month", "Year", "Average_Delay")
    .orderBy($"Year", $"Month", $"Average_Delay".desc)

  mostDelayedDayEachMonth.show()

  // Save the most delayed day for each month and year to a CSV file in HDFS
  mostDelayedDayEachMonth.coalesce(1).write.option("header", "true").csv("daily_trend/most_delayed_day_monthly")

  spark.stop()
