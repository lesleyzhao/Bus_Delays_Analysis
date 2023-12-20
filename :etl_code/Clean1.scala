
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

// Start a Spark session 
val spark = SparkSession.builder.appName("DataCleaningInteractive").getOrCreate()

// Read the data with inferred schema and header
var df = spark.read.option("header", "true").option("inferSchema", "true").csv("Bus_Breakdown_and_Delays_20231105.csv")

// Select only the columns of interest
var columnsOfInterest = Array("School_Year","Route_Number", "Reason","Occurred_On", "Boro", "Bus_Company_Name", "How_Long_Delayed","Number_Of_Students_On_The_Bus")
// Clean the data
var columnsToDrop = df.columns.diff(columnsOfInterest)
// Now drop these columns from the DataFrame
df = df.drop(columnsToDrop: _*)


var finalDf = df
  .na.drop(columnsOfInterest)
  .filter(!lower(col("How_Long_Delayed")).equalTo("nan"))

// Format How_Long_Delayed_Clean to keep only numeric value
finalDf = finalDf.withColumn("How_Long_Delayed_Clean",
  substring(regexp_replace(col("How_Long_Delayed"), "[^0-9]", ""), 1, 2).cast("int"))

// Drop NaN for two columns
finalDf = finalDf.na.drop(Seq("Number_Of_Students_On_The_Bus", "How_Long_Delayed_Clean"))


// Format the "Occurred_On" column
// I format all dates from previous version 06/08/2016 02:12:00 to 2016-06-08 02:12:00
finalDf = finalDf.withColumn("Occurred_On_Formatted",
  unix_timestamp(col("Occurred_On"), "MM/dd/yyyy hh:mm:ss a").cast("timestamp"))

// Format "company" column
// I remove all characters after ‘(’ in the "Bus_Company_Name" column and convert all letters to lowercases
finalDf = finalDf.withColumn(
  "Bus_Company_Name_Normalized",
  regexp_replace(
    lower(regexp_extract(col("Bus_Company_Name"), "^(.*?)(\\s*\\(.*|$)", 1)),
    "[\"']", ""
  )
)


finalDf.show()
// Print the number of rows in finalDf
println(s"Number of rows in finalDf: ${finalDf.count()}")
// Check if there are any rows with NaN values
var containsCaseInsensitiveNaN = finalDf.columns.exists(column => {
  if (finalDf.schema(column).dataType == DoubleType) {
    finalDf.filter(col(column).isNull || col(column).isNaN).count() > 0
  } else {
    finalDf.filter(col(column).isNull).count() > 0
  }
})
println(s"Does the final DataFrame contain NaN values? $containsCaseInsensitiveNaN")


// Convert the columns to DoubleType (or IntegerType, as appropriate)
finalDf = finalDf
  .withColumn("How_Long_Delayed_Clean", col("How_Long_Delayed_Clean").cast(DoubleType))
  .withColumn("Number_Of_Students_On_The_Bus", col("Number_Of_Students_On_The_Bus").cast(DoubleType))




// For "Boro" column
val boroIndexer = new StringIndexer().setInputCol("Boro").setOutputCol("BoroIndex").setHandleInvalid("skip")
val boroIndexerModel = boroIndexer.fit(finalDf)
val dfWithBoroIndex = boroIndexerModel.transform(finalDf)

// Print the mapping for "Boro"
val boroLabels = boroIndexerModel.labels
println("Boro category index mapping:")
boroLabels.zipWithIndex.foreach { case (label, index) => println(s"$label -> $index") }

// For "Reason" column
val reasonIndexer = new StringIndexer().setInputCol("Reason").setOutputCol("ReasonIndex").setHandleInvalid("skip")
val reasonIndexerModel = reasonIndexer.fit(dfWithBoroIndex)
val indexedDF = reasonIndexerModel.transform(dfWithBoroIndex)

// Print the mapping for "Reason"
val reasonLabels = reasonIndexerModel.labels
println("Reason category index mapping:")
reasonLabels.zipWithIndex.foreach { case (label, index) => println(s"$label -> $index") }




// Define VectorAssembler
val assembler = new VectorAssembler()
  .setInputCols(Array("How_Long_Delayed_Clean", "Number_Of_Students_On_The_Bus", "BoroIndex", "ReasonIndex"))
  .setOutputCol("features")

// Show the first 10 rows of indexedDF
indexedDF.show(10)


// Write the cleaned dataframe to HDFS
indexedDF.coalesce(1).write.option("header", "true").csv("Bus_Cleaned")

// Stop the Spark session
spark.stop()

