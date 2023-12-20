// Question 3: analyze the most common reasons for delay
// predict delay duration based on the hour of the day and borough


import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types._
import org.apache.spark.ml.tuning.{TrainValidationSplit, ParamGridBuilder}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.feature.VectorAssembler


val spark = SparkSession.builder.appName("Delay Analysis and Prediction").getOrCreate()

// Schema definition based on the dataset structure
val schema = new StructType()
  .add("School_Year", StringType, true)
  .add("Route_Number", StringType, true)
  .add("Reason", StringType, true)
  .add("Occurred_On", StringType, true) // Assuming the original format is a string
  .add("Boro", StringType, true)
  .add("Bus_Company_Name", StringType, true)
  .add("How_Long_Delayed", StringType, true)
  .add("Number_Of_Students_On_The_Bus", DoubleType, true)
  .add("How_Long_Delayed_Clean", DoubleType, true)
  .add("Occurred_On_Formatted", StringType, true) // Assuming the original format is a string
  .add("Bus_Company_Name_Normalized", StringType, true)
  .add("BoroIndex", DoubleType, true)
  .add("ReasonIndex", DoubleType, true)

// Load your DataFrame here using the schema defined above
val df = spark.read.schema(schema).option("header", "true").csv("Bus_Cleaned/part-00000-e1e78d81-df57-4af1-a07d-0ed189bb09b9-c000.csv")


// Converting Occurred_On_Formatted from String to Timestamp if necessary
val dfWithTimestamp = df.withColumn("Occurred_On_Formatted", to_timestamp($"Occurred_On_Formatted"))

// Extract hour from Occurred_On_Formatted after ensuring it is in Timestamp format
val dfWithHour = dfWithTimestamp.withColumn("Hour", hour($"Occurred_On_Formatted"))

// Assemble features into a feature vector
val assembler = new VectorAssembler()
  .setInputCols(Array("BoroIndex", "ReasonIndex", "Hour", "Number_Of_Students_On_The_Bus"))
  .setOutputCol("features")

// Transform the DataFrame to include the 'features' column
val assembledData = assembler.transform(dfWithHour)

// Define the RandomForestRegressor
val rf = new RandomForestRegressor()
  .setLabelCol("How_Long_Delayed_Clean")
  .setFeaturesCol("features")

// Split the data into training and test sets
val Array(trainingData, testData) = assembledData.randomSplit(Array(0.8, 0.2))

// ParamGrid for hyperparameter tuning
val paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(10, 20))
  .addGrid(rf.maxDepth, Array(5, 10))
  .build()

// CrossValidator
val cv = new CrossValidator()
  .setEstimator(rf)
  .setEvaluator(new RegressionEvaluator().setLabelCol("How_Long_Delayed_Clean"))
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)  // Use 3+ in practice

// Run cross-validation, and choose the best set of parameters.
val cvModel = cv.fit(trainingData)

// Make predictions on test data
val predictions = cvModel.transform(testData)

// Select example rows to display
predictions.select("prediction", "How_Long_Delayed_Clean", "features").show(5)

// Select (prediction, true label) and compute test error
val evaluator = new RegressionEvaluator()
  .setLabelCol("How_Long_Delayed_Clean")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

// Stop the Spark session
spark.stop()
