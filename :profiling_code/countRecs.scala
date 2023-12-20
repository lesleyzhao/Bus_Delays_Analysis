
import org.apache.spark.sql.SparkSession
import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType

// Put whatever filename we have in HDFS Bus_Cleaned directory here.
// IMPORTANT: Notice the exact filename may change if you reexecute the previous ETL code
var df = spark.read.option("header", "true").csv("Bus_Cleaned/part-00000-e1e78d81-df57-4af1-a07d-0ed189bb09b9-c000.csv")
// Count the number of records in the DataFrame
val recordCount = df.count()
println(s"Total records: $recordCount")


// Define columns of interest at the beginning
val columnsOfInterest = Seq("School_Year","Route_Number", "Reason","Occurred_On_Formatted", "Boro", "Bus_Company_Name_Normalized", "How_Long_Delayed_Clean","Number_Of_Students_On_The_Bus")
// Explore dataset and schema:
println("Explore dataset and schema: ")
df.printSchema()

// Maximum and Minimum Values for Each Column
println("Maximum and Minimum Values for Each Column: ")
columnsOfInterest.foreach { columnName =>
  val minMax = df.agg(min(col(columnName)).alias("min"), max(col(columnName)).alias("max"))
  minMax.show()
}

// 3. Longest and Shortest Line in Free-Format Text Columns (e.g., "Reason")
val textColumns = Seq("Reason", "Bus_Company_Name_Normalized") // Add more if applicable
textColumns.foreach { columnName =>
  val lengthStats = df.select(
    col(columnName),
    length(col(columnName)).alias("length")
  ).agg(
    max("length").alias("max_length"),
    min("length").alias("min_length")
  )
  println(s"Length stats for $columnName:")
  lengthStats.show()
}

//  Range of Values for Each Column (for numerical columns)
println("Range of Values for Numerical Columns: ")
val numericalColumns = Seq("How_Long_Delayed_Clean", "Number_Of_Students_On_The_Bus") // Add more if applicable
numericalColumns.foreach { columnName =>
  val range = df.select(max(col(columnName)).minus(min(col(columnName))).alias("range"))
  range.show()
}

//  Frequency of Each Value in a Column
println("Frequency of Each Value in Each Column: ")
columnsOfInterest.foreach { columnName =>
  val valueFrequency = df.groupBy(col(columnName)).count().orderBy(col("count").desc)
  println(s"Value frequency for $columnName:")
  valueFrequency.show()
}


// df is original DataFrame, map and count:
println("Map and count based on school-year: ")
val schoolYearCounts = df.rdd
  // Map each row to a tuple of (School-Year, 1)
  .map(row => (row.getAs[String]("School_Year"), 1))
  // Reduce by key, which is the school-year, summing up the 1's for each school-year
  .reduceByKey(_ + _)

// Collect and print the results
schoolYearCounts.collect().foreach(println)


//println("Count distinct values in each column: ")
//val limitNumber = 10
println("Count distinct values in each column: ")
columnsOfInterest.foreach { columnName =>
  val distinctCount = df.select(columnName).distinct().count()
  println(s"Count of distinct values in $columnName: $distinctCount")
}




// Convert necessary columns to Integer type
val dfTyped = df.withColumn("Number_Of_Students_On_The_Bus", col("Number_Of_Students_On_The_Bus").cast(IntegerType))
               .withColumn("How_Long_Delayed_Clean", col("How_Long_Delayed_Clean").cast(IntegerType))

// Calculate mean, median, mode, and standard deviation for How_Long_Delayed_Clean
val delayStats = dfTyped
  .select(
    mean(col("How_Long_Delayed_Clean")).alias("mean_delay"),
    expr("percentile_approx(How_Long_Delayed_Clean, 0.5)").alias("median_delay"),
    stddev(col("How_Long_Delayed_Clean")).alias("stddev_delay")
  )


// Calculate mode separately since Spark doesn't have a built-in function for mode
val delayMode = dfTyped.groupBy("How_Long_Delayed_Clean")
  .count()
  .orderBy(col("count").desc)
  .first()
  .getAs[Int]("How_Long_Delayed_Clean")

// Calculate mean, median, mode, and standard deviation for Number_Of_Students_On_The_Bus
val studentsStats = dfTyped
  .select(
    mean(col("Number_Of_Students_On_The_Bus")).alias("mean_students"),
    expr("percentile_approx(Number_Of_Students_On_The_Bus, 0.5)").alias("median_students"),
    stddev(col("Number_Of_Students_On_The_Bus")).alias("stddev_students")
  )


// Calculate mode separately for Number_Of_Students_On_The_Bus
val studentsMode = dfTyped.groupBy("Number_Of_Students_On_The_Bus")
  .count()
  .orderBy(col("count").desc)
  .first()
  .getAs[Int]("Number_Of_Students_On_The_Bus")

// Save these stats into a new file
val delayStatsWithMode = delayStats.withColumn("mode_delay", lit(delayMode))
val studentsStatsWithMode = studentsStats.withColumn("mode_students", lit(studentsMode))

// Combine both statistics DataFrames into one
val combinedStats = delayStatsWithMode.crossJoin(studentsStatsWithMode)

// Show the combined statistics DataFrame
combinedStats.show()
//Save the combined statistics DataFrame to HDFS Bus_Profiling_Stats directory
combinedStats.coalesce(1).write
  .option("header", "true")
  .csv("Bus_Profiling_Stats")


// Display the statistics
delayStats.show()
println(s"Mode of How_Long_Delayed_Clean: $delayMode")
studentsStats.show()
println(s"Mode of Number_Of_Students_On_The_Bus: $studentsMode")

spark.stop()